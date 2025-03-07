import sys
import json
import time
import os
import re
from datetime import datetime

# PyQt5
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QPushButton, QTextEdit, QFileDialog, QLineEdit, QLabel, QMessageBox,
    QHBoxLayout, QCheckBox, QDialog, QGridLayout, QSystemTrayIcon
)

from PyQt5.QtCore import Qt, QSettings
from PyQt5.QtGui import QIcon

# Biblioteca docling, para extrair texto do PDF
from docling.document_converter import DocumentConverter

# Biblioteca oficial do Google Generative AI (Gemini/PaLM)
import google.generativeai as genai
from google.generativeai.types import GenerationConfig

# Biblioteca para PostgreSQL
import psycopg2

# Importando o módulo repoSQL para geração de script SQL
try:
    from repoSQL import ContractParser, generate_sql_script
    REPO_SQL_AVAILABLE = True
except ImportError:
    REPO_SQL_AVAILABLE = False
    print("AVISO: Módulo repoSQL não encontrado. A funcionalidade de geração de SQL não estará disponível.")

# Importando o diálogo de configuração do banco de dados
from DialogConfiguracaoBD import DialogConfiguracaoBD

# Importando funções para execução SQL
from execute_sql_script import DB_CONFIG
from s3_upload import upload_file_to_s3, S3_CONFIG


###############################################################################
# CONFIGURAÇÕES
###############################################################################

CONFIG = {
    "gemini": {
        "api_key": "AIzaSyAykyWUpxP0KUh_JhLtYMKl0gXq1IzzWBY",
        "model": "gemini-2.0-flash"           
    }
}

###############################################################################
# 1) Função que constrói o prompt
###############################################################################
def build_main_prompt(text_block: str, pdf_filename: str) -> str:
    """
    Retorna o prompt completo, instruindo o LLM a extrair dados e retornar SOMENTE JSON.
    
    Args:
        text_block: Texto do PDF a ser analisado
        pdf_filename: Nome do arquivo PDF para incluir no anexo_contrato
    """
    return f"""
Você é um especialista em análise de contratos de compras públicas. 
Sua tarefa é:
1) Identificar e extrair o máximo de metadados possíveis, seguindo as regras abaixo.
2) Dividir o texto em chunks coerentes para evitar perda de informações ou omissões.
3) Retornar SOMENTE o JSON no formato especificado, sem comentários nem texto extra.

Formato do JSON a retornar (sem nenhuma alteração):
{{
  "numero_contrato": "XXXXXXXXX",
  "tipo_instrumento": "Contrato",
  "processo_administrativo": "XXXXXXXXX",
  "data_celebracao": "DD/MM/AAAA",
  "orgao_contratante": {{
    "razao_social": "Nome do Órgão",
    "sigla": "SIGLA_DO_ÓRGÃO",
    "cnpj": "XX.XXX.XXX/XXXX-XX"
  }},
  "empresa_contratada": {{
    "razao_social": "Nome da Empresa",
    "cnpj": "XX.XXX.XXX/XXXX-XX"
  }},
  "itens": [
    {{
      "descricao": "Nome do Item",
      "especificacao": "Detalhes Técnicos",
      "unidade_medida": "Unidade de Fornecimento (ex: unidade, litro, metro)",
      "quantidade": "Número total de unidades adquiridas",
      "valor_unitario": "R$ X,XX",
      "valor_total": "R$ X.XXX,XX",
      "catmat_catser": "Código CATMAT/CATSER",
      "tipo": "Material",  
      "locais_execucao_entrega": "Cidade (UF), Cidade (UF)"
    }}
  ],
  "fonte_preco": "Contrato",
  "referencia_contrato": "Número do contrato de onde os preços foram extraídos",
  "anexo_contrato": "s3://compras-ia-np/Contratos/{pdf_filename}",
  "status_extracao": "Sucesso"
}}

Regras de extração:
- Extraia número, data, partes contratantes (com CNPJ e sigla), itens (descrição, quantidade, valores, etc.), e o que mais constar.
- Divida em chunks coerentes caso o texto seja extenso, para não perder informações importantes.
- Se algum dado não existir ou você ficar em dúvida sobre o que extrair em razão de algum formato confuso ou não esperado, informe "Parcial" em "status_extracao", mas não invente valores. 
  Caso os dados estejam em formato de tabela com células mescladas, aplique as seguintes regras:
  (i) se for código do item, considere o código da célula mesclada para todos os itens que possuem esta célular mesclada.
  (ii) se for preço total do item, desconsidere o que está no documento e retorne o resultado do cálculo qtde de item * valor unitário.
  (iii) se for outro tipo de dado como descrição, por exemplo, repita o valor qdo campo ue estiver na célula mesclada para o campo correspondente dos itens que contém esta célula.
- *Número do contrato*: localizar no cabeçalho do documento.
- *Tipo de instrumento*: se for um contrato, informar "Contrato"; caso contrário, extrair o nome exato.
- *Data de celebração*: se o documento for gerado via SEI, buscar a data no final; caso contrário, extrair do cabeçalho ou cláusulas iniciais.
- *Órgão contratante*: extrair a razão social, a sigla (se existir) e o CNPJ.
- *Empresa contratada*: extrair a razão social e o CNPJ.
- *Itens*: descrição, espec. técnica, unidade, quantidade, valor unitário/total, etc.
- *Fonte do preço*: "Contrato".
- *Referência do contrato*: repetir o número do contrato utilizado.
- *Anexo do contrato*: use EXATAMENTE "s3://compras-ia-np/Contratos/{pdf_filename}" sem alterações.
- *Status da extração*: "Sucesso" se tudo for extraído, "Parcial" se faltar algo.

Texto a analisar:
-----
{text_block}
-----

É EXTREMAMENTE IMPORTANTE que você retorne APENAS o JSON válido, sem nenhum texto adicional, explicação ou formatação markdown.
"""


###############################################################################
# 2) Função que analisa texto com GEMINI usando a versão correta da API
###############################################################################
def analyze_with_gemini(text: str, pdf_path: str):
    """
    Analisa o texto usando a API Google Generative AI (Gemini).
    Retorna (json_dict, tempo_execucao, raw_response).
    Se falhar o parsing JSON, retorna (None, tempo_execucao, raw_response).
    
    Args:
        text: Texto do PDF a ser analisado
        pdf_path: Caminho do arquivo PDF para extrair o nome do arquivo
    """
    start_time = time.time()
    raw_output = None

    try:
        # Extrair apenas o nome do arquivo do caminho completo
        pdf_filename = os.path.basename(pdf_path)
        
        # 1) Configurar a API Key
        genai.configure(api_key=CONFIG['gemini']['api_key'])

        # 2) Montar prompt com o nome do arquivo
        prompt = build_main_prompt(text, pdf_filename)

        # 3) Chamar a API usando GenerativeModel (método mais recente)
        model = genai.GenerativeModel(model_name=CONFIG['gemini']['model'])
        
        # Configurar parâmetros de geração
        generation_config = GenerationConfig(
            temperature=0.0,
            top_p=0.95,
            top_k=40,
            max_output_tokens=8192  # Aumentado para permitir respostas maiores
        )
        
        # Fazer a chamada da API
        response = model.generate_content(
            contents=prompt,
            generation_config=generation_config
        )
        
        elapsed = time.time() - start_time
        
        # 6) Extrair o texto da resposta (formato para generate_content)
        if hasattr(response, 'text'):
            raw_output = response.text
        elif hasattr(response, 'parts') and response.parts:
            raw_output = response.parts[0].text
        elif hasattr(response, 'candidates') and response.candidates:
            if hasattr(response.candidates[0], 'content') and hasattr(response.candidates[0].content, 'parts'):
                raw_output = response.candidates[0].content.parts[0].text
            else:
                raw_output = str(response.candidates[0])
        else:
            # Última tentativa - tentar acessar como string ou representação direta
            try:
                raw_output = str(response)
            except:
                print("Não foi possível extrair texto da resposta. Formato desconhecido.")
                print(f"Resposta: {response}")
                return None, elapsed, raw_output
                
        # Verificar se o output não está vazio
        if not raw_output or raw_output.strip() == "":
            print("Resposta vazia recebida do modelo Gemini.")
            return None, elapsed, raw_output
            
        print(f"Resposta bruta do Gemini (primeiros 200 caracteres): {raw_output[:200]}")

        # 7) Limpar a resposta - remover blocos de código se existirem
        cleaned_output = raw_output.strip()
        print(f"Resposta original antes da limpeza: '{cleaned_output[:50]}...'")
        
        # Remover marcadores Markdown de forma simples
        if cleaned_output.startswith("```json"):
            cleaned_output = cleaned_output.replace("```json", "", 1)
            
        if cleaned_output.endswith("```"):
            cleaned_output = cleaned_output[:cleaned_output.rindex("```")]
            
        # Remover outras ocorrências de ``` se existirem
        cleaned_output = cleaned_output.replace("```", "").strip()
        
        print(f"Após limpeza: '{cleaned_output[:50]}...'")
        
        # Verificar se há conteúdo após a limpeza
        if not cleaned_output or cleaned_output.isspace():
            print("Conteúdo vazio após limpeza!")
            return None, elapsed, raw_output
            
        # 8) Analisar como JSON
        try:
            # Primeiro verifica se há algum conteúdo antes de tentar analisar
            if not cleaned_output or cleaned_output.isspace():
                print("Conteúdo vazio após limpeza!")
                return None, elapsed, raw_output
                
            # Tenta encontrar um objeto JSON na string (qualquer coisa entre chaves)
            import re
            json_pattern = r'(\{.*\})'
            json_matches = re.search(json_pattern, cleaned_output, re.DOTALL)
            
            if json_matches:
                json_content = json_matches.group(1).strip()
                print(f"Encontrado JSON potencial: '{json_content[:50]}...'")
                try:
                    parsed_json = json.loads(json_content)
                    print("Análise de JSON bem-sucedida!")
                    
                    # Verificar se o anexo_contrato está correto, senão corrigir
                    expected_anexo = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                    if parsed_json.get("anexo_contrato") != expected_anexo:
                        parsed_json["anexo_contrato"] = expected_anexo
                        print(f"Campo anexo_contrato corrigido para: {expected_anexo}")
                    
                    return parsed_json, elapsed, raw_output
                except json.JSONDecodeError as e:
                    print(f"Encontrado texto que parece JSON, mas não é válido: {json_content[:100]}...")
                    print(f"Erro específico: {str(e)}")
            
            # Última tentativa: Verificar se existem caracteres estranhos no início
            # e tentar analisar o JSON ignorando-os
            for i in range(len(cleaned_output)):
                if cleaned_output[i] == '{':
                    potential_json = cleaned_output[i:]
                    try:
                        parsed_json = json.loads(potential_json)
                        print(f"Sucesso ao remover {i} caracteres do início!")
                        
                        # Verificar se o anexo_contrato está correto, senão corrigir
                        expected_anexo = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                        if parsed_json.get("anexo_contrato") != expected_anexo:
                            parsed_json["anexo_contrato"] = expected_anexo
                            print(f"Campo anexo_contrato corrigido para: {expected_anexo}")
                        
                        return parsed_json, elapsed, raw_output
                    except:
                        pass
            
            # Se não encontrou um padrão JSON ou falhou ao analisar, tenta analisar o texto completo
            parsed_json = json.loads(cleaned_output)
            
            # Verificar se o anexo_contrato está correto, senão corrigir
            expected_anexo = f"s3://compras-ia-np/Contratos/{pdf_filename}"
            if parsed_json.get("anexo_contrato") != expected_anexo:
                parsed_json["anexo_contrato"] = expected_anexo
                print(f"Campo anexo_contrato corrigido para: {expected_anexo}")
            
            return parsed_json, elapsed, raw_output
            
        except json.JSONDecodeError as e:
            print(f"Erro ao analisar JSON: {str(e)}")
            print(f"Conteúdo recebido (até 300 caracteres): '{cleaned_output[:300]}'")
            
            # Tentativa final: criar um JSON manualmente com a resposta do modelo
            fallback_json = {
                "status_extracao": "Falha",
                "mensagem": "Não foi possível extrair dados estruturados",
                "resposta_modelo": cleaned_output[:1000] + ("..." if len(cleaned_output) > 1000 else ""),
                "anexo_contrato": f"s3://compras-ia-np/Contratos/{pdf_filename}"
            }
            return fallback_json, elapsed, raw_output
            
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"Erro na API do Gemini: {str(e)}")
        return None, elapsed, raw_output


###############################################################################
# 3) Função para exportar JSON para arquivo
###############################################################################
def save_json_to_file(json_data, pdf_path):
    """
    Salva o resultado JSON em um arquivo.
    Retorna o caminho do arquivo salvo.
    """
    try:
        # Criar nome de arquivo baseado no nome do PDF e timestamp
        if pdf_path:
            base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        else:
            base_name = "documento"
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"{base_name}_{timestamp}.json"
        
        # Garantir que o diretório de saída existe
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
        os.makedirs(output_dir, exist_ok=True)
        
        # Caminho completo do arquivo
        json_filepath = os.path.join(output_dir, json_filename)
        
        # Salvar o JSON com formatação
        with open(json_filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
            
        return json_filepath
        
    except Exception as e:
        print(f"Erro ao salvar arquivo JSON: {str(e)}")
        return None


###############################################################################
# 4) Função para gerar e salvar o script SQL
###############################################################################
def generate_and_save_sql(json_data, pdf_path):
    """
    Gera um script SQL usando o módulo repoSQL e salva em um arquivo.
    Retorna o caminho do arquivo SQL salvo.
    """
    if not REPO_SQL_AVAILABLE:
        print("Módulo repoSQL não disponível. Não é possível gerar script SQL.")
        return None
    
    try:
        # Obter o nome do arquivo PDF
        pdf_filename = os.path.basename(pdf_path) if pdf_path else "documento.pdf"
        
        # Converter o JSON para uma string formatada
        json_str = json.dumps(json_data, ensure_ascii=False)
        
        # Parsear o JSON para objeto Contrato usando o ContractParser
        contrato = ContractParser.parse(json_str)
        
        # Gerar o script SQL
        sql_script = generate_sql_script(contrato, pdf_filename)
        
        # Criar nome de arquivo SQL
        base_name = os.path.splitext(os.path.basename(pdf_path))[0] if pdf_path else "documento"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sql_filename = f"{base_name}_{timestamp}.sql"
        
        # Garantir que o diretório de saída existe
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
        os.makedirs(output_dir, exist_ok=True)
        
        # Caminho completo do arquivo SQL
        sql_filepath = os.path.join(output_dir, sql_filename)
        
        # Salvar o script SQL
        with open(sql_filepath, 'w', encoding='utf-8') as f:
            f.write(sql_script)
            
        return sql_filepath
        
    except Exception as e:
        print(f"Erro ao gerar/salvar script SQL: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


###############################################################################
# 5) Classe principal PyQt para GUI: abrir PDF, extrair texto e chamar Gemini
###############################################################################
class DoclingGeminiApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Docling + Google Generative AI (JSON Extractor)")
        self.setGeometry(100, 100, 800, 600)

        # Armazenar a resposta bruta do Gemini
        self.raw_gemini_response = None
        self.current_json_result = None
        self.current_pdf_path = None
        self.current_json_filepath = None
        
        # Lista para armazenar múltiplos PDFs selecionados
        self.pdf_files_queue = []

        # Verificar versão da biblioteca
        try:
            import google.generativeai as genai
            print(f"Versão da biblioteca google.generativeai: {genai.__version__}")
        except (ImportError, AttributeError):
            print("Não foi possível determinar a versão da biblioteca google.generativeai.")

        # Widget central e layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout()
        central_widget.setLayout(layout)

        # Label + QLineEdit para mostrar caminho/URL PDF
        self.label_pdf = QLabel("Caminho/URL do PDF:")
        layout.addWidget(self.label_pdf)

        self.line_pdf = QLineEdit()
        layout.addWidget(self.line_pdf)

        # Botão para abrir PDF local
        self.btn_open_pdf = QPushButton("Selecionar PDFs...")
        self.btn_open_pdf.clicked.connect(self.select_pdf_dialog)
        layout.addWidget(self.btn_open_pdf)

        # Checkbox para opções
        options_layout = QHBoxLayout()
        layout.addLayout(options_layout)
        
        self.check_show_raw = QCheckBox("Mostrar resposta bruta do Gemini (para debug)")
        options_layout.addWidget(self.check_show_raw)
        
        self.check_generate_sql = QCheckBox("Gerar script SQL")
        self.check_generate_sql.setChecked(REPO_SQL_AVAILABLE)
        self.check_generate_sql.setEnabled(REPO_SQL_AVAILABLE)
        options_layout.addWidget(self.check_generate_sql)
        
        # Adicionar botão de configuração do banco de dados no layout de opções
        options_layout.addStretch()
        self.btn_config_db = QPushButton("Configurar PostgreSQL")
        self.btn_config_db.clicked.connect(self.show_db_config_dialog)
        options_layout.addWidget(self.btn_config_db)
        
        # Adicionar botão de configuração do S3 no layout de opções
        self.btn_config_s3 = QPushButton("Configurar Amazon S3")
        self.btn_config_s3.clicked.connect(self.show_s3_config_dialog)
        options_layout.addWidget(self.btn_config_s3)

        # Botão para processar (Docling + LLM)
        buttons_layout = QHBoxLayout()
        layout.addLayout(buttons_layout)
        
        self.btn_process = QPushButton("Converter e enviar ao Gemini")
        self.btn_process.clicked.connect(self.process_pdf)  # Conexão correta ao método process_pdf
        buttons_layout.addWidget(self.btn_process)
        
        self.btn_process_all = QPushButton("Processar todos os PDFs")
        self.btn_process_all.clicked.connect(self.process_all_pdfs)
        self.btn_process_all.setVisible(False)  # Inicialmente oculto
        buttons_layout.addWidget(self.btn_process_all)
        
        self.btn_save_json = QPushButton("Salvar JSON em arquivo")
        self.btn_save_json.clicked.connect(self.save_json)
        self.btn_save_json.setEnabled(False)  # Desabilitado até ter um JSON
        buttons_layout.addWidget(self.btn_save_json)
        
        self.btn_generate_sql = QPushButton("Gerar Script SQL")
        self.btn_generate_sql.clicked.connect(self.generate_sql)
        self.btn_generate_sql.setEnabled(False)  # Desabilitado até ter um JSON
        self.btn_generate_sql.setVisible(REPO_SQL_AVAILABLE)  # Só mostrar se o módulo estiver disponível
        buttons_layout.addWidget(self.btn_generate_sql)
        
        # Adicionar botão para executar SQL
        self.btn_execute_sql = QPushButton("Executar SQL no PostgreSQL")
        self.btn_execute_sql.clicked.connect(self.execute_sql_direct)
        self.btn_execute_sql.setEnabled(False)  # Desabilitado inicialmente
        buttons_layout.addWidget(self.btn_execute_sql)
        
        # Adicionar botão para upload manual para S3
        self.btn_upload_to_s3 = QPushButton("Enviar PDF para S3")
        self.btn_upload_to_s3.clicked.connect(self.manual_upload_to_s3)
        self.btn_upload_to_s3.setEnabled(False)  # Desabilitado inicialmente
        buttons_layout.addWidget(self.btn_upload_to_s3)

        # Área de texto para exibir resultado JSON ou erros
        self.text_result = QTextEdit()
        self.text_result.setReadOnly(True)
        layout.addWidget(self.text_result)

        # Instancia converter docling
        self.doc_converter = DocumentConverter()
        
        # Carregar configurações do app
        self.settings = QSettings("Docling", "GeminiExtractor")
        
        # Carregar configurações S3
        self.s3_settings = QSettings("Docling", "GeminiExtractor")
        self.update_s3_config()
        
        # Atualizar as configurações do banco de dados a partir das configurações salvas
        self.update_db_config()
        
        # Variável para controlar se o SQL foi executado
        self.sql_executed = False
        self.last_sql_filepath = None
        
        # Inicializar ícone de bandeja do sistema (se suportado)
        self.setup_system_tray()

    def setup_system_tray(self):
        """Configura o ícone de bandeja do sistema para notificações"""
        try:
            self.tray_icon = QSystemTrayIcon()
            # Se o ícone não estiver disponível, usar um ícone padrão ou nenhum ícone
            try:
                icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon.png")
                if os.path.exists(icon_path):
                    self.tray_icon.setIcon(QIcon(icon_path))
                else:
                    self.tray_icon.setIcon(QIcon.fromTheme("document-save"))
            except:
                pass
                
            self.tray_icon.setVisible(True)
        except Exception as e:
            print(f"Não foi possível configurar o ícone de bandeja: {str(e)}")
            self.tray_icon = None

    def show_notification(self, title, message):
        """Mostra uma notificação do sistema"""
        try:
            # Opção 1: Usar ícone de bandeja do sistema se disponível
            if hasattr(self, 'tray_icon') and self.tray_icon:
                self.tray_icon.showMessage(title, message, QSystemTrayIcon.Information, 5000)
                return True
                
            # Opção 2: Tentar usar plyer para notificações desktop (cross-platform)
            try:
                from plyer import notification
                notification.notify(
                    title=title,
                    message=message,
                    app_name="Compras IA",
                    timeout=10
                )
                return True
            except ImportError:
                pass
            
            # Opção 3: Cair para QMessageBox se nada mais funcionar
            QMessageBox.information(self, title, message)
            return True
        except Exception as e:
            print(f"Erro ao mostrar notificação: {str(e)}")
            return False

    def log_upload_event(self, success, file_path, s3_key, message):
        """Registra evento de upload no log para rastreamento"""
        try:
            # Criar diretório de logs se não existir
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
            os.makedirs(log_dir, exist_ok=True)
            
            # Abrir arquivo de log em modo append
            log_file = os.path.join(log_dir, "s3_uploads.log")
            with open(log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                status = "SUCCESS" if success else "FAILURE"
                s3_key = s3_key or os.path.basename(file_path)
                
                f.write(f"{timestamp} | {status} | {file_path} | {s3_key} | {message}\n")
            
            return True
        except Exception as e:
            print(f"Erro ao registrar upload: {str(e)}")
            return False

    def update_db_config(self):
        """Atualiza as configurações do banco de dados a partir das configurações salvas"""
        # Importar modulo global
        from execute_sql_script import DB_CONFIG
        
        # Atualizar as configurações globais do banco de dados
        DB_CONFIG["host"] = self.settings.value("db/host", "54.175.112.114")
        DB_CONFIG["port"] = int(self.settings.value("db/port", 5433))
        DB_CONFIG["database"] = self.settings.value("db/database", "compras_ia")
        DB_CONFIG["user"] = self.settings.value("db/user", "compras")
        DB_CONFIG["password"] = self.settings.value("db/password", "Sinerji")

    def show_db_config_dialog(self):
        """Mostra o diálogo de configuração do banco de dados"""
        dialog = DialogConfiguracaoBD(self)
        result = dialog.exec_()
        
        if result == QDialog.Accepted:
            # Salva as configurações
            dialog.save_settings()
            
            # Atualiza as configurações globais
            self.update_db_config()
            
            QMessageBox.information(
                self,
                "Configurações Salvas",
                "As configurações do PostgreSQL foram salvas com sucesso."
            )

    def select_pdf_dialog(self):
        """Abre caixa de diálogo de arquivo para selecionar múltiplos PDFs."""
        pdf_paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Selecionar PDFs",
            "",
            "Arquivos PDF (*.pdf)"
        )
        
        if pdf_paths:
            # Armazenar a lista de PDFs selecionados
            self.pdf_files_queue = pdf_paths
            
            # Mostrar os PDFs na caixa de texto
            self.line_pdf.setText("; ".join(pdf_paths))
            
            # Atualizar a interface para mostrar quantos PDFs foram selecionados
            self.text_result.setPlainText(f"{len(pdf_paths)} PDFs selecionados para processamento.")
            
            # Configurar o primeiro PDF como o atual
            self.current_pdf_path = pdf_paths[0]
            self.btn_upload_to_s3.setEnabled(True)
            
            # Habilitar botão de processar todos se houver mais de um PDF
            if len(pdf_paths) > 1:
                self.btn_process_all.setVisible(True)
            else:
                self.btn_process_all.setVisible(False)

    def update_s3_config(self):
        """Atualiza as configurações do S3 a partir das configurações salvas"""
        # Importar configurações globais do S3
        from s3_upload import S3_CONFIG
        
        # Atualizar as configurações S3
        S3_CONFIG["aws_access_key_id"] = self.s3_settings.value("s3/access_key", "")
        S3_CONFIG["aws_secret_access_key"] = self.s3_settings.value("s3/secret_key", "")
        S3_CONFIG["region_name"] = self.s3_settings.value("s3/region", "us-east-1")
        S3_CONFIG["bucket_name"] = self.s3_settings.value("s3/bucket", "compras-ia-np")
        S3_CONFIG["notify_uploads"] = self.s3_settings.value("s3/notify_uploads", "true") == "true"
        S3_CONFIG["notification_email"] = self.s3_settings.value("s3/notification_email", "")
        S3_CONFIG["webhook_url"] = self.s3_settings.value("s3/webhook_url", "")

    def show_s3_config_dialog(self):
        """Mostra o diálogo de configuração do S3"""
        dialog = QDialog(self)
        dialog.setWindowTitle("Configuração da Amazon S3")
        dialog.setMinimumWidth(400)
        
        layout = QVBoxLayout()
        
        # Campos de configuração em grid
        grid = QGridLayout()
        
        # Access Key
        grid.addWidget(QLabel("Access Key ID:"), 0, 0)
        edit_access_key = QLineEdit(self.s3_settings.value("s3/access_key", ""))
        grid.addWidget(edit_access_key, 0, 1)
        
        # Secret Key
        grid.addWidget(QLabel("Secret Access Key:"), 1, 0)
        edit_secret_key = QLineEdit(self.s3_settings.value("s3/secret_key", ""))
        edit_secret_key.setEchoMode(QLineEdit.Password)
        grid.addWidget(edit_secret_key, 1, 1)
        
        # Region
        grid.addWidget(QLabel("Região:"), 2, 0)
        edit_region = QLineEdit(self.s3_settings.value("s3/region", "us-east-1"))
        grid.addWidget(edit_region, 2, 1)
        
        # Bucket
        grid.addWidget(QLabel("Bucket:"), 3, 0)
        edit_bucket = QLineEdit(self.s3_settings.value("s3/bucket", "compras-ia-np"))
        grid.addWidget(edit_bucket, 3, 1)
        
        # Email para notificações
        grid.addWidget(QLabel("Email para notificações:"), 4, 0)
        edit_email = QLineEdit(self.s3_settings.value("s3/notification_email", ""))
        grid.addWidget(edit_email, 4, 1)
        
        # Webhook URL (opcional)
        grid.addWidget(QLabel("URL Webhook (opcional):"), 5, 0)
        edit_webhook = QLineEdit(self.s3_settings.value("s3/webhook_url", ""))
        grid.addWidget(edit_webhook, 5, 1)
        
        layout.addLayout(grid)
        
        # Checkboxes para opções
        check_auto_upload = QCheckBox("Enviar arquivos para S3 automaticamente após processar")
        check_auto_upload.setChecked(self.s3_settings.value("s3/auto_upload", "true") == "true")
        layout.addWidget(check_auto_upload)
        
        check_notify = QCheckBox("Receber notificações para uploads")
        check_notify.setChecked(self.s3_settings.value("s3/notify_uploads", "true") == "true")
        layout.addWidget(check_notify)
        
        # Botões
        button_layout = QHBoxLayout()
        
        btn_test = QPushButton("Testar Conexão")
        btn_test.clicked.connect(lambda: self.test_s3_connection(
            edit_access_key.text(),
            edit_secret_key.text(),
            edit_region.text(),
            edit_bucket.text()
        ))
        button_layout.addWidget(btn_test)
        
        btn_save = QPushButton("Salvar")
        btn_save.clicked.connect(lambda: self.save_s3_settings(
            dialog,
            edit_access_key.text(),
            edit_secret_key.text(),
            edit_region.text(),
            edit_bucket.text(),
            edit_email.text(),
            edit_webhook.text(),
            check_auto_upload.isChecked(),
            check_notify.isChecked()
        ))
        button_layout.addWidget(btn_save)
        
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(dialog.reject)
        button_layout.addWidget(btn_cancel)
        
        layout.addLayout(button_layout)
        
        dialog.setLayout(layout)
        dialog.exec_()

    def test_s3_connection(self, access_key, secret_key, region, bucket):
        """Testa a conexão com o S3 usando as configurações fornecidas"""
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            # Inicializar cliente S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            
            # Tentar listar objetos no bucket para testar a conexão
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                MaxKeys=1
            )
            
            QMessageBox.information(
                self,
                "Sucesso",
                f"Conexão com a Amazon S3 estabelecida com sucesso!\nBucket: {bucket}"
            )
            
        except ClientError as e:
            QMessageBox.critical(
                self,
                "Erro de Conexão",
                f"Não foi possível conectar ao S3:\n{str(e)}"
            )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Erro",
                f"Erro ao testar conexão com S3:\n{str(e)}"
            )

    def save_s3_settings(self, dialog, access_key, secret_key, region, bucket, email, webhook, auto_upload, notify_uploads):
        """Salva as configurações do S3"""
        self.s3_settings.setValue("s3/access_key", access_key)
        self.s3_settings.setValue("s3/secret_key", secret_key)
        self.s3_settings.setValue("s3/region", region)
        self.s3_settings.setValue("s3/bucket", bucket)
        self.s3_settings.setValue("s3/notification_email", email)
        self.s3_settings.setValue("s3/webhook_url", webhook)
        self.s3_settings.setValue("s3/auto_upload", "true" if auto_upload else "false")
        self.s3_settings.setValue("s3/notify_uploads", "true" if notify_uploads else "false")
        self.s3_settings.sync()
        
        # Atualizar configurações globais
        self.update_s3_config()
        
        dialog.accept()
        
        QMessageBox.information(
            self,
            "Configurações Salvas",
            "As configurações da Amazon S3 foram salvas com sucesso."
        )

    def upload_to_s3(self, file_path, s3_key=None):
        """
        Envia um arquivo para o S3 e atualiza a interface com notificações.
        
        Args:
            file_path (str): Caminho do arquivo local
            s3_key (str, optional): Caminho/nome no S3. Se None, usa o nome do arquivo
        """
        try:
            self.text_result.append("\n\nEnviando arquivo para Amazon S3...")
            QApplication.processEvents()
            
            # Importar função de upload
            from s3_upload import upload_file_to_s3
            
            # Se o s3_key não for especificado e for um arquivo PDF
            if s3_key is None and file_path.lower().endswith('.pdf'):
                pdf_filename = os.path.basename(file_path)
                s3_key = f"Contratos/{pdf_filename}"
            
            # Fazer upload
            success, message = upload_file_to_s3(file_path, s3_key)
            
            if success:
                self.text_result.append(f"\n✅ {message}")
                
                # Mostrar notificação de sucesso
                self.show_notification(
                    "Upload Concluído",
                    f"Arquivo enviado com sucesso para S3: {s3_key or os.path.basename(file_path)}"
                )
                
                # Registrar o upload bem-sucedido
                self.log_upload_event(True, file_path, s3_key, message)
            else:
                self.text_result.append(f"\n❌ {message}")
                
                QMessageBox.warning(
                    self,
                    "Aviso",
                    f"Erro ao enviar arquivo para S3:\n{message}"
                )
                
            return success
            
        except Exception as e:
            error_message = f"Erro ao enviar arquivo para S3: {str(e)}"
            self.text_result.append(f"\n❌ {error_message}")
            
            QMessageBox.critical(
                self,
                "Erro",
                error_message
            )
            return False

    def manual_upload_to_s3(self):
        """Upload manual do PDF atual para o S3"""
        if not self.current_pdf_path:
            QMessageBox.warning(self, "Aviso", "Nenhum PDF selecionado para upload.")
            return
        
        try:
            # Extrair o nome do arquivo PDF
            pdf_filename = os.path.basename(self.current_pdf_path)
            s3_key = f"Contratos/{pdf_filename}"
            
            # Perguntar ao usuário se deseja prosseguir
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Question)
            msg.setText(f"Deseja enviar o PDF {pdf_filename} para o Amazon S3?")
            msg.setInformativeText(f"Será enviado para s3://compras-ia-np/Contratos/{pdf_filename}")
            msg.setWindowTitle("Confirmar upload")
            msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            
            if msg.exec_() == QMessageBox.Yes:
                # Fazer upload do PDF para o S3
                success = self.upload_to_s3(self.current_pdf_path, s3_key)
                
                if success and self.current_json_result:
                    # Atualizar o campo de s3_url no JSON se necessário
                    self.current_json_result["anexo_contrato"] = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                    
                    # Salvar o JSON atualizado se houver um arquivo JSON atual
                    if hasattr(self, 'current_json_filepath') and self.current_json_filepath:
                        try:
                            with open(self.current_json_filepath, 'w', encoding='utf-8') as f:
                                json.dump(self.current_json_result, f, indent=2, ensure_ascii=False)
                                
                            self.text_result.append(f"\nJSON atualizado com o caminho S3 correto.")
                        except Exception as e:
                            self.text_result.append(f"\nErro ao atualizar JSON: {str(e)}")
                    
                    # Mostrar a mensagem de sucesso
                    QMessageBox.information(
                        self,
                        "Sucesso",
                        f"PDF enviado com sucesso para a S3 e\ncampo 'anexo_contrato' atualizado no JSON."
                    )
        
        except Exception as e:
            QMessageBox.critical(
                self,
                "Erro",
                f"Erro ao fazer upload manual para S3: {str(e)}"
            )

    def execute_sql_direct(self):
        """Executa o SQL diretamente no PostgreSQL e faz upload para S3."""
        if not self.last_sql_filepath or not os.path.exists(self.last_sql_filepath):
            QMessageBox.warning(
                self,
                "Aviso",
                "Não há script SQL gerado para executar."
            )
            return
        
        try:
            # Mostrar mensagem de processamento
            self.text_result.append("\n\nExecutando SQL no PostgreSQL...")
            QApplication.processEvents()
            
            # Ler o arquivo SQL
            with open(self.last_sql_filepath, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # Mostrar os primeiros 500 caracteres do script
            preview = sql_script[:500] + "..." if len(sql_script) > 500 else sql_script
            self.text_result.append(f"\nScript a ser executado (preview):\n{preview}")
            QApplication.processEvents()
            
            # Obter configurações de conexão
            host = self.settings.value("db/host", "54.175.112.114")
            port = int(self.settings.value("db/port", 5433))
            database = self.settings.value("db/database", "compras_ia")
            user = self.settings.value("db/user", "compras") 
            password = self.settings.value("db/password", "Sinerji")
            
            # Conectar ao PostgreSQL
            import psycopg2
            
            self.text_result.append(f"\nConectando ao PostgreSQL em {host}:{port}, banco {database}...")
            QApplication.processEvents()
            
            connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            # Criar um cursor e executar o script
            results = []
            with connection.cursor() as cursor:
                try:
                    # Tentar executar o script completo
                    cursor.execute(sql_script)
                    affected_rows = cursor.rowcount
                    results.append(f"Script executado com sucesso: {affected_rows} linhas afetadas")
                except Exception as script_error:
                    # Se falhar ao executar tudo de uma vez, dividir por comandos
                    connection.rollback()  # Desfaz qualquer alteração parcial
                    
                    self.text_result.append("\nExecução completa falhou. Tentando executar comando por comando...")
                    QApplication.processEvents()
                    
                    # Este regex divide por ponto e vírgula, mas ignora os que estão dentro de strings
                    import re
                    sql_commands = re.split(r';(?=(?:[^\']*\'[^\']*\')*[^\']*$)', sql_script)
                    
                    # Contador de comandos bem-sucedidos
                    success_count = 0
                    error_count = 0
                    
                    for i, command in enumerate(sql_commands):
                        cmd = command.strip()
                        if cmd:  # Ignorar comandos vazios
                            try:
                                cursor.execute(cmd)
                                affected_rows = cursor.rowcount
                                results.append(f"Comando {i+1}: {affected_rows} linhas afetadas")
                                success_count += 1
                            except Exception as cmd_error:
                                # Registrar erro específico do comando
                                error_msg = str(cmd_error)
                                results.append(f"Erro no comando {i+1}: {error_msg}")
                                error_count += 1
                    
                    # Resumo da execução
                    results.append(f"Resumo: {success_count} comandos bem-sucedidos, {error_count} erros")
                    
                    # Se todos os comandos falharam, considerar como falha geral
                    if success_count == 0:
                        raise Exception(f"Todos os {error_count} comandos falharam")
                
                # Commit das alterações
                connection.commit()
                
            # Fechar conexão
            connection.close()
            
            # Atualizar interface com resultados
            result_message = "\n".join(results)
            self.text_result.append(f"\n✅ Script SQL executado com sucesso.\n{result_message}")
            self.sql_executed = True
            
            # Verificar se deve fazer upload do PDF para S3 após execução SQL bem-sucedida
            if self.current_pdf_path and os.path.exists(self.current_pdf_path) and self.sql_executed:
                auto_upload = self.s3_settings.value("s3/auto_upload", "true") == "true"
                
                # Se o upload automático está ativado ou se o PDF ainda não foi enviado
                if auto_upload:
                    # Verificar se o arquivo já existe no S3
                    from s3_upload import check_file_exists_in_s3
                    
                    pdf_filename = os.path.basename(self.current_pdf_path)
                    s3_key = f"Contratos/{pdf_filename}"
                    
                    file_exists = False
                    try:
                        file_exists = check_file_exists_in_s3(s3_key)
                    except:
                        file_exists = False
                    
                    # Se o arquivo não existe no S3, fazer upload
                    if not file_exists:
                        self.text_result.append(f"\nSQL executado com sucesso. Enviando PDF para Amazon S3...")
                        QApplication.processEvents()
                        
                        # Fazer upload do PDF
                        success = self.upload_to_s3(self.current_pdf_path, s3_key)
                        
                        # Se o upload foi bem-sucedido, atualizar o JSON se necessário
                        if success and self.current_json_result:
                            expected_anexo = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                            if self.current_json_result.get("anexo_contrato") != expected_anexo:
                                self.current_json_result["anexo_contrato"] = expected_anexo
                                
                                # Atualizar o JSON em arquivo se existir
                                if hasattr(self, 'current_json_filepath') and self.current_json_filepath and os.path.exists(self.current_json_filepath):
                                    try:
                                        with open(self.current_json_filepath, 'w', encoding='utf-8') as f:
                                            json.dump(self.current_json_result, f, indent=2, ensure_ascii=False)
                                            
                                        self.text_result.append(f"JSON atualizado com anexo_contrato: {expected_anexo}")
                                    except Exception as e:
                                        self.text_result.append(f"Erro ao atualizar JSON após upload: {str(e)}")
                    else:
                        self.text_result.append(f"\nArquivo PDF já existe no S3: {s3_key}")
            
            QMessageBox.information(
                self,
                "Sucesso",
                "Script SQL executado com sucesso no PostgreSQL."
            )
        
        except Exception as e:
            error_message = f"Erro ao executar SQL: {str(e)}"
            self.text_result.append(f"\n❌ {error_message}")
            
            # Se a conexão foi estabelecida, tentar fechar
            if 'connection' in locals() and connection:
                try:
                    connection.rollback()  # Tentar rollback para garantir
                    connection.close()
                except:
                    pass
            
            QMessageBox.critical(
                self,
                "Erro",
                error_message
            )
        
        except Exception as e:
            error_message = f"Erro ao executar SQL: {str(e)}"
            self.text_result.append(f"\n❌ {error_message}")
            
        # Se a conexão foi estabelecida, tentar fechar
        if 'connection' in locals() and connection:
            try:
                connection.rollback()  # Tentar rollback para garantir
                connection.close()
            except:
                pass
            
        QMessageBox.critical(
                self,
                "Erro",
                #error_message
            )

    def execute_sql_script(self, sql_filepath):
        """
        Executa um script SQL a partir de um arquivo.
        Retorna (sucesso, mensagem).
        """
        try:
            # Ler o arquivo SQL
            with open(sql_filepath, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # Obter configurações de conexão
            host = self.settings.value("db/host", "54.175.112.114")
            port = int(self.settings.value("db/port", 5433))
            database = self.settings.value("db/database", "compras_ia")
            user = self.settings.value("db/user", "compras") 
            password = self.settings.value("db/password", "Sinerji")
            
            # Conectar ao PostgreSQL
            import psycopg2
            
            connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            # Executar o script
            results = []
            with connection.cursor() as cursor:
                try:
                    # Tentar executar o script completo
                    cursor.execute(sql_script)
                    affected_rows = cursor.rowcount
                    results.append(f"Script executado com sucesso: {affected_rows} linhas afetadas")
                except Exception as script_error:
                    # Se falhar, tentar comando por comando
                    connection.rollback()
                    
                    # Dividir o script em comandos por ";"
                    import re
                    sql_commands = re.split(r';(?=(?:[^\']*\'[^\']*\')*[^\']*$)', sql_script)
                    
                    success_count = 0
                    error_count = 0
                    
                    for i, command in enumerate(sql_commands):
                        cmd = command.strip()
                        if cmd:
                            try:
                                cursor.execute(cmd)
                                success_count += 1
                            except Exception as cmd_error:
                                error_count += 1
                    
                    # Se todos falharam, considerar falha
                    if success_count == 0:
                        raise Exception(f"Todos os {error_count} comandos falharam")
                
                # Commit
                connection.commit()
                
            # Fechar conexão
            connection.close()
            
            return True, "SQL executado com sucesso"
            
        except Exception as e:
            # Se a conexão existe, fechá-la
            if 'connection' in locals() and connection:
                try:
                    connection.rollback()
                    connection.close()
                except:
                    pass
                    
            return False, str(e)

    def process_all_pdfs(self):
        """Processa todos os PDFs na fila, um após o outro."""
        if not hasattr(self, 'pdf_files_queue') or not self.pdf_files_queue:
            QMessageBox.warning(self, "Aviso", "Nenhum PDF selecionado para processamento.")
            return
        
        total_pdfs = len(self.pdf_files_queue)
        self.text_result.setPlainText(f"Iniciando processamento de {total_pdfs} PDFs...")
        QApplication.processEvents()
        
        # Desabilitar botões durante o processamento em lote
        self.btn_process.setEnabled(False)
        self.btn_process_all.setEnabled(False)
        self.btn_open_pdf.setEnabled(False)
        
        success_count = 0
        error_count = 0
        results_summary = []
        
        # Processar cada PDF na fila
        for i, pdf_path in enumerate(self.pdf_files_queue):
            try:
                self.text_result.append(f"\n\n{'='*50}")
                self.text_result.append(f"Processando PDF {i+1}/{total_pdfs}: {os.path.basename(pdf_path)}")
                self.text_result.append(f"{'='*50}\n")
                QApplication.processEvents()
                
                # Definir o PDF atual
                self.current_pdf_path = pdf_path
                
                # Extrair texto usando Docling
                self.text_result.append("Extraindo texto do PDF...")
                QApplication.processEvents()
                
                docling_result = self.doc_converter.convert(pdf_path)
                extracted_text = docling_result.document.export_to_markdown()
                
                if not extracted_text.strip():
                    self.text_result.append("❌ O PDF não contém texto extraível ou está vazio.")
                    error_count += 1
                    results_summary.append(f"❌ {os.path.basename(pdf_path)}: Sem texto extraível")
                    continue
                
                # Analisar com Gemini
                self.text_result.append(f"Texto extraído ({len(extracted_text)} caracteres). Enviando para Gemini...")
                QApplication.processEvents()
                
                json_result, tempo, raw_response = analyze_with_gemini(extracted_text, pdf_path)
                self.raw_gemini_response = raw_response
                
                if json_result is None:
                    self.text_result.append("❌ Não foi possível obter um JSON válido do Gemini.")
                    error_count += 1
                    results_summary.append(f"❌ {os.path.basename(pdf_path)}: Falha na geração de JSON")
                    continue
                
                # Salvar o JSON
                self.current_json_result = json_result
                json_filepath = save_json_to_file(json_result, pdf_path)
                if json_filepath:
                    self.text_result.append(f"✅ JSON salvo em: {json_filepath}")
                    self.current_json_filepath = json_filepath
                
                # Verificar se deve fazer upload automático do PDF para S3
                auto_upload = self.s3_settings.value("s3/auto_upload", "true") == "true"
                if auto_upload:
                    # Extrair o nome do arquivo PDF
                    pdf_filename = os.path.basename(pdf_path)
                    s3_key = f"Contratos/{pdf_filename}"
                    
                    # Fazer upload do PDF para o S3
                    self.text_result.append(f"\nEnviando PDF para o Amazon S3: {pdf_filename}")
                    QApplication.processEvents()
                    
                    # Chamar a função de upload
                    success = self.upload_to_s3(pdf_path, s3_key)
                    
                    # Se o upload foi bem-sucedido, certificar-se de que o JSON tem o anexo_contrato correto
                    if success and json_filepath and json_result:
                        json_result["anexo_contrato"] = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                        
                        # Atualizar o JSON em arquivo
                        try:
                            with open(json_filepath, 'w', encoding='utf-8') as f:
                                json.dump(json_result, f, indent=2, ensure_ascii=False)
                        except Exception as e:
                            self.text_result.append(f"❌ Erro ao atualizar JSON após upload: {str(e)}")
                
                # Gerar SQL se necessário
                if self.check_generate_sql.isChecked() and REPO_SQL_AVAILABLE:
                    try:
                        sql_filepath = generate_and_save_sql(json_result, pdf_path)
                        if sql_filepath:
                            self.last_sql_filepath = sql_filepath
                            self.text_result.append(f"✅ SQL gerado em: {sql_filepath}")
                            
                            # Executar SQL se configurado para automático
                            auto_exec = self.settings.value("db/auto_exec", "true") == "true"
                            if auto_exec:
                                self.text_result.append("Executando SQL automaticamente...")
                                QApplication.processEvents()
                                success, message = self.execute_sql_script(sql_filepath)
                                if success:
                                    self.text_result.append(f"✅ SQL executado com sucesso")
                                else:
                                    self.text_result.append(f"❌ Erro ao executar SQL: {message}")
                    except Exception as e_sql:
                        self.text_result.append(f"❌ Erro ao gerar/executar SQL: {str(e_sql)}")
                        
                success_count += 1
                results_summary.append(f"✅ {os.path.basename(pdf_path)}: Processado com sucesso")
                    
            except Exception as e:
                error_message = f"Erro ao processar '{os.path.basename(pdf_path)}': {str(e)}"
                self.text_result.append(f"❌ {error_message}")
                error_count += 1
                results_summary.append(f"❌ {os.path.basename(pdf_path)}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        # Resumo final
        self.text_result.append(f"\n\n{'='*50}")
        self.text_result.append(f"RESUMO DO PROCESSAMENTO")
        self.text_result.append(f"{'='*50}")
        self.text_result.append(f"Total de PDFs: {total_pdfs}")
        self.text_result.append(f"Sucesso: {success_count}")
        self.text_result.append(f"Falhas: {error_count}")
        self.text_result.append(f"\nResultados individuais:")
        for result in results_summary:
            self.text_result.append(f"- {result}")
        
        # Reabilitar botões
        self.btn_process.setEnabled(True)
        self.btn_process_all.setEnabled(True)
        self.btn_open_pdf.setEnabled(True)

    def save_json(self):
        """Salva o JSON atual em um arquivo."""
        if not self.current_json_result:
            QMessageBox.warning(self, "Aviso", "Não há resultados JSON para salvar.")
            return
            
        try:
            # Verificar se o campo anexo_contrato está no formato correto
            if self.current_pdf_path and "anexo_contrato" in self.current_json_result:
                pdf_filename = os.path.basename(self.current_pdf_path)
                expected_anexo = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                if self.current_json_result["anexo_contrato"] != expected_anexo:
                    self.current_json_result["anexo_contrato"] = expected_anexo
            
            json_filepath = save_json_to_file(self.current_json_result, self.current_pdf_path)
            
            if json_filepath:
                self.current_json_filepath = json_filepath
                
                QMessageBox.information(
                    self, 
                    "Sucesso", 
                    f"Arquivo JSON salvo com sucesso em:\n{json_filepath}"
                )
                
                # Se a opção de gerar SQL estiver marcada, gerar o SQL também
                if self.check_generate_sql.isChecked() and REPO_SQL_AVAILABLE:
                    self.generate_sql()
            else:
                QMessageBox.warning(self, "Erro", "Não foi possível salvar o arquivo JSON.")
                
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Erro", 
                f"Erro ao salvar arquivo: {str(e)}"
            )

    def generate_sql(self):
        """Gera e salva um script SQL a partir do JSON atual."""
        if not self.current_json_result:
            QMessageBox.warning(self, "Aviso", "Não há resultados JSON para gerar SQL.")
            return
            
        if not REPO_SQL_AVAILABLE:
            QMessageBox.warning(self, "Aviso", "Módulo repoSQL não disponível. Não é possível gerar script SQL.")
            return
            
        try:
            # Gerar e salvar o script SQL
            sql_filepath = generate_and_save_sql(self.current_json_result, self.current_pdf_path)
            
            if sql_filepath:
                self.last_sql_filepath = sql_filepath
                self.btn_execute_sql.setEnabled(True)
                
                QMessageBox.information(
                    self, 
                    "Sucesso", 
                    f"Script SQL gerado com sucesso em:\n{sql_filepath}"
                )
                
                # Mostrar o caminho do arquivo SQL no texto de resultado
                self.text_result.append(f"\n\nScript SQL gerado em: {sql_filepath}")
                
                # Verificar se deve executar automaticamente
                auto_exec = self.settings.value("db/auto_exec", "true") == "true"
                if auto_exec:
                    self.text_result.append("\nExecutando SQL automaticamente...")
                    QApplication.processEvents()
                    self.execute_sql_direct()
            else:
                QMessageBox.warning(self, "Erro", "Não foi possível gerar o script SQL.")
                
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Erro", 
                f"Erro ao gerar script SQL: {str(e)}"
            )

    def try_repair_truncated_json(self, raw_response):
        """
        Tenta reparar um JSON truncado a partir da resposta bruta do modelo.
        Retorna o JSON reparado ou None se não for possível reparar.
        """
        try:
            # Extrair apenas o nome do arquivo se o caminho do PDF estiver disponível
            pdf_filename = os.path.basename(self.current_pdf_path) if self.current_pdf_path else "documento.pdf"
            
            # Remover blocos de código markdown se existirem
            cleaned = raw_response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned.replace("```json", "", 1)
            if cleaned.endswith("```"):
                cleaned = cleaned[:cleaned.rindex("```")]
            cleaned = cleaned.replace("```", "").strip()
            
            # Verificar se o JSON está completo
            try:
                # Tenta analisar como está
                parsed_json = json.loads(cleaned)
                
                # Garantir o campo anexo_contrato correto
                parsed_json["anexo_contrato"] = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                
                return parsed_json
            except json.JSONDecodeError:
                # Se não for válido, tenta reparar
                pass
                
            # Verificar se o JSON tem parte inicial válida
            import re
            json_start_pattern = r'(\{\s*"[^"]+"\s*:)'
            if not re.search(json_start_pattern, cleaned):
                # Não parece JSON válido
                return None
                
            # Tentar encontrar chaves de abertura/fechamento
            open_braces = cleaned.count('{')
            close_braces = cleaned.count('}')
            
            if open_braces > close_braces:
                # JSON truncado - adicionar chaves de fechamento
                missing_braces = open_braces - close_braces
                cleaned += "}" * missing_braces
            
            # Verificar se há outras incompletudes comuns
            # Se há uma propriedade sem valor/vírgula
            last_property_pattern = r',\s*"[^"]+"\s*:\s*$'
            if re.search(last_property_pattern, cleaned):
                cleaned += ' "Parcial"'
                
            # Se termina com vírgula
            if cleaned.rstrip().endswith(","):
                # Remove a vírgula final
                cleaned = cleaned.rstrip().rstrip(",") + "\n}"
                
            # Tenta novamente analisar como JSON
            try:
                parsed_json = json.loads(cleaned)
                
                # Garantir o campo anexo_contrato correto
                parsed_json["anexo_contrato"] = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                
                return parsed_json
            except json.JSONDecodeError as e:
                print(f"Não foi possível reparar JSON: {str(e)}")
                return None
                
        except Exception as e:
            print(f"Erro ao tentar reparar JSON: {str(e)}")
            return None

    def process_pdf(self):
        """Extrai texto via docling e chama o LLM (Gemini) para obter JSON."""
        pdf_source = self.line_pdf.text().strip()
        if not pdf_source:
            QMessageBox.warning(self, "Aviso", "Selecione um PDF ou informe uma URL.")
            return

        # Se há vários PDFs separados por ponto-e-vírgula, usar apenas o primeiro
        if ";" in pdf_source:
            pdf_source = pdf_source.split(";")[0].strip()

        self.current_pdf_path = pdf_source
        self.current_json_result = None
        self.raw_gemini_response = None
        self.btn_save_json.setEnabled(False)
        self.btn_generate_sql.setEnabled(False)
        self.btn_execute_sql.setEnabled(False)
        self.btn_upload_to_s3.setEnabled(True)  # Habilitar o botão de upload
        self.last_sql_filepath = None
        
        self.text_result.clear()
        self.text_result.setPlainText("Processando... Por favor, aguarde.")
        QApplication.processEvents()  # Atualiza a interface para mostrar a mensagem

        try:
            # 1) Extrair texto do PDF usando Docling
            self.text_result.setPlainText("Extraindo texto do PDF...")
            QApplication.processEvents()
            
            docling_result = self.doc_converter.convert(pdf_source)
            extracted_text = docling_result.document.export_to_markdown()
            
            # Mostrar progresso
            self.text_result.setPlainText(f"Texto extraído ({len(extracted_text)} caracteres).\nEnviando para análise com Gemini...")
            QApplication.processEvents()
            
            # 2) Se o texto for vazio, mostrar mensagem de erro
            if not extracted_text.strip():
                self.text_result.setPlainText("O PDF não contém texto extraível ou está vazio.")
                return
                
            # 3) Analisar o texto com Gemini (agora passando o caminho do PDF)
            json_result, tempo, raw_response = analyze_with_gemini(extracted_text, pdf_source)
            self.raw_gemini_response = raw_response
            
            # 4) Mostrar resultados
            if json_result is None:
                self.text_result.setPlainText(
                    "Não foi possível obter um JSON válido do Gemini.\n\n"
                    "Possíveis causas:\n"
                    "1. Problema na chamada da API do Gemini\n"
                    "2. Resposta inválida (não-JSON) do modelo\n"
                    "3. Texto muito grande ou complexo\n\n"
                    "Recomendações:\n"
                    "- Verifique a chave da API e o modelo selecionado\n"
                    "- Tente novamente com um documento menor ou mais simples\n"
                    "- Veja logs no console para detalhes técnicos\n\n"
                    f"Primeiros 500 caracteres do texto extraído:\n{extracted_text[:500]}..."
                )
                
                # Mostrar resposta bruta se solicitado
                if self.check_show_raw.isChecked() and self.raw_gemini_response:
                    self.text_result.append("\n\n--- RESPOSTA BRUTA DO GEMINI ---\n")
                    self.text_result.append(self.raw_gemini_response)
                
            elif json_result.get("status_extracao") == "Falha":
                # Tenta reparar o JSON truncado e mostrar o resultado
                self.text_result.setPlainText(
                    "O JSON retornado pelo modelo parece ter sido truncado.\n"
                    "Tentando reparar e processar mesmo assim...\n\n"
                )
                
                # Tentar extrair JSON completo da resposta bruta
                if self.raw_gemini_response:
                    try:
                        repaired_json = self.try_repair_truncated_json(self.raw_gemini_response)
                        if repaired_json:
                            pretty_json = json.dumps(repaired_json, indent=2, ensure_ascii=False)
                            self.text_result.append(f"JSON reparado com sucesso!\n\n{pretty_json}")
                            
                            # Atualizar o JSON atual com a versão reparada
                            self.current_json_result = repaired_json
                            
                            # Salvar automaticamente o JSON reparado
                            try:
                                json_filepath = save_json_to_file(repaired_json, self.current_pdf_path)
                                if json_filepath:
                                    self.current_json_filepath = json_filepath
                                    self.text_result.append(f"\n\nJSON reparado salvo em: {json_filepath}")
                                    
                                    # Verificar se deve fazer upload automático do PDF para S3
                                    auto_upload = self.s3_settings.value("s3/auto_upload", "true") == "true"
                                    if auto_upload:
                                        # Extrair o nome do arquivo PDF
                                        pdf_filename = os.path.basename(pdf_source)
                                        s3_key = f"Contratos/{pdf_filename}"
                                        
                                        # Fazer upload do PDF para o S3
                                        self.text_result.append(f"\nEnviando PDF para o Amazon S3: {pdf_filename}")
                                        QApplication.processEvents()
                                        
                                        # Chamar a função de upload
                                        success = self.upload_to_s3(pdf_source, s3_key)
                                    
                                    # Gerar SQL se a opção estiver marcada
                                    if self.check_generate_sql.isChecked() and REPO_SQL_AVAILABLE:
                                        try:
                                            sql_filepath = generate_and_save_sql(repaired_json, self.current_pdf_path)
                                            if sql_filepath:
                                                self.last_sql_filepath = sql_filepath
                                                self.btn_execute_sql.setEnabled(True)
                                                self.text_result.append(f"\n\nScript SQL gerado em: {sql_filepath}")
                                                
                                                # Verificar se deve executar automaticamente
                                                auto_exec = self.settings.value("db/auto_exec", "true") == "true"
                                                if auto_exec:
                                                    self.text_result.append("\nExecutando SQL automaticamente...")
                                                    QApplication.processEvents()
                                                    self.execute_sql_direct()
                                        except Exception as e_sql:
                                            self.text_result.append(f"\n\nErro ao gerar SQL: {str(e_sql)}")
                            except Exception as e:
                                self.text_result.append(f"\n\nErro ao salvar JSON reparado: {str(e)}")
                        else:
                            self.text_result.append("\nNão foi possível reparar o JSON truncado.\n\n" + 
                                json_result.get("resposta_modelo", "Sem resposta"))
                    except Exception as e:
                        self.text_result.append(f"\nErro ao tentar reparar JSON: {str(e)}\n\n" + 
                            json_result.get("resposta_modelo", "Sem resposta"))
                else:
                    self.text_result.append("Resposta do modelo:\n\n" + 
                        json_result.get("resposta_modelo", "Sem resposta"))
                
                # Salvar mesmo o JSON de fallback
                self.current_json_result = json_result
                self.btn_save_json.setEnabled(True)
                self.btn_generate_sql.setEnabled(REPO_SQL_AVAILABLE)
                
                # Mostrar resposta bruta se solicitado
                if self.check_show_raw.isChecked() and self.raw_gemini_response:
                    self.text_result.append("\n\n--- RESPOSTA BRUTA DO GEMINI ---\n")
                    self.text_result.append(self.raw_gemini_response)
                
            else:
                # Garantir que o campo anexo_contrato está correto
                pdf_filename = os.path.basename(pdf_source)
                json_result["anexo_contrato"] = f"s3://compras-ia-np/Contratos/{pdf_filename}"
                
                # Formatar o JSON com indentação para melhor visualização
                pretty_json = json.dumps(json_result, indent=2, ensure_ascii=False)
                
                # Verificar se o formato está correto
                json_validate_msg = self.validate_json_structure(json_result)
                
                self.text_result.setPlainText(
                    f"Análise concluída em {tempo:.2f}s\n\n"
                    f"{json_validate_msg}\n\n"
                    f"{pretty_json}"
                )
                
                # Mostrar resposta bruta se solicitado
                if self.check_show_raw.isChecked() and self.raw_gemini_response:
                    self.text_result.append("\n\n--- RESPOSTA BRUTA DO GEMINI ---\n")
                    self.text_result.append(self.raw_gemini_response)
                
                # Salvar para uso posterior
                self.current_json_result = json_result
                self.btn_save_json.setEnabled(True)
                self.btn_generate_sql.setEnabled(REPO_SQL_AVAILABLE)
                
                # Salvar automaticamente em arquivo
                try:
                    json_filepath = save_json_to_file(json_result, pdf_source)
                    if json_filepath:
                        self.current_json_filepath = json_filepath
                        self.text_result.append(f"\n\nJSON salvo automaticamente em: {json_filepath}")
                        
                        # Verificar se deve fazer upload automático do PDF para S3
                        auto_upload = self.s3_settings.value("s3/auto_upload", "true") == "true"
                        if auto_upload:
                            # Extrair o nome do arquivo PDF
                            pdf_filename = os.path.basename(pdf_source)
                            s3_key = f"Contratos/{pdf_filename}"
                            
                            # Fazer upload do PDF para o S3
                            self.text_result.append(f"\nEnviando PDF para o Amazon S3...")
                            QApplication.processEvents()
                            
                            # Chamar a função de upload
                            success = self.upload_to_s3(pdf_source, s3_key)
                        
                        # Gerar SQL se a opção estiver marcada
                        if self.check_generate_sql.isChecked() and REPO_SQL_AVAILABLE:
                            try:
                                sql_filepath = generate_and_save_sql(json_result, pdf_source)
                                if sql_filepath:
                                    self.last_sql_filepath = sql_filepath
                                    self.btn_execute_sql.setEnabled(True)
                                    self.text_result.append(f"\n\nScript SQL gerado em: {sql_filepath}")
                                    
                                    # Verificar se deve executar automaticamente
                                    auto_exec = self.settings.value("db/auto_exec", "true") == "true"
                                    if auto_exec:
                                        self.text_result.append("\nExecutando SQL automaticamente...")
                                        QApplication.processEvents()
                                        self.execute_sql_direct()
                            except Exception as e_sql:
                                self.text_result.append(f"\n\nErro ao gerar SQL: {str(e_sql)}")
                except Exception as e:
                    self.text_result.append(f"\n\nErro ao salvar JSON: {str(e)}")
                
        except Exception as e:
            self.text_result.setPlainText(
                f"Erro ao processar: {str(e)}\n\n"
                "Detalhes do erro foram impressos no console para debug."
            )
            import traceback
            traceback.print_exc()  # Imprime stack trace no console
    
    def validate_json_structure(self, json_data):
        """
        Valida se o JSON retornado pelo Gemini está no formato esperado.
        Retorna uma mensagem sobre a validação.
        """
        expected_keys = [
            "numero_contrato", "tipo_instrumento", "processo_administrativo", 
            "data_celebracao", "orgao_contratante", "empresa_contratada", 
            "itens", "fonte_preco", "referencia_contrato", 
            "anexo_contrato", "status_extracao"
        ]
        
        # Verificar campos principais
        missing_keys = [key for key in expected_keys if key not in json_data]
        
        # Verificar subcampos de orgao_contratante
        orgao_keys = ["razao_social", "sigla", "cnpj"]
        if "orgao_contratante" in json_data:
            missing_orgao = [key for key in orgao_keys if key not in json_data["orgao_contratante"]]
        else:
            missing_orgao = orgao_keys
            
        # Verificar subcampos de empresa_contratada
        empresa_keys = ["razao_social", "cnpj"]
        if "empresa_contratada" in json_data:
            missing_empresa = [key for key in empresa_keys if key not in json_data["empresa_contratada"]]
        else:
            missing_empresa = empresa_keys
            
        # Verificar array de itens
        if "itens" in json_data and isinstance(json_data["itens"], list) and json_data["itens"]:
            item_keys = [
                "descricao", "especificacao", "unidade_medida", "quantidade",
                "valor_unitario", "valor_total", "catmat_catser", "tipo",
                "locais_execucao_entrega"
            ]
            missing_item = [key for key in item_keys if key not in json_data["itens"][0]]
        else:
            missing_item = []
            
        # Compor mensagem final
        if not missing_keys and not missing_orgao and not missing_empresa and not missing_item:
            return "✅ JSON retornado está no formato correto!"
        else:
            msg_parts = []
            
            if missing_keys:
                msg_parts.append(f"❌ Campos principais ausentes: {', '.join(missing_keys)}")
                
            if missing_orgao:
                msg_parts.append(f"❌ Campos ausentes em 'orgao_contratante': {', '.join(missing_orgao)}")
                
            if missing_empresa:
                msg_parts.append(f"❌ Campos ausentes em 'empresa_contratada': {', '.join(missing_empresa)}")
                
            if missing_item:
                msg_parts.append(f"❌ Campos ausentes nos itens: {', '.join(missing_item)}")
                
            return "AVISO: Formato do JSON retornado não está completo:\n" + "\n".join(msg_parts)
            
###############################################################################
# 5) Função main - inicia a GUI
###############################################################################
def main():
    app = QApplication(sys.argv)
    window = DoclingGeminiApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()