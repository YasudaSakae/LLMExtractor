import sys
import json
import time
import os
from datetime import datetime

# PyQt5
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QPushButton, QTextEdit, QFileDialog, QLineEdit, QLabel, QMessageBox,
    QHBoxLayout, QCheckBox
)

from PyQt5.QtCore import Qt

# Biblioteca docling, para extrair texto do PDF
from docling.document_converter import DocumentConverter

# Biblioteca oficial do Google Generative AI (Gemini/PaLM)
import google.generativeai as genai
from google.generativeai.types import GenerationConfig


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
Você é um especialista em análise de documentos públicos.
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
- Se algum dado não existir, informe "Parcial" em "status_extracao", mas não invente valores.
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
# 4) Classe principal PyQt para GUI: abrir PDF, extrair texto e chamar Gemini
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
        self.btn_open_pdf = QPushButton("Selecionar PDF local...")
        self.btn_open_pdf.clicked.connect(self.select_pdf_dialog)
        layout.addWidget(self.btn_open_pdf)

        # Checkbox para mostrar resposta bruta
        self.check_show_raw = QCheckBox("Mostrar resposta bruta do Gemini (para debug)")
        layout.addWidget(self.check_show_raw)

        # Botão para processar (Docling + LLM)
        buttons_layout = QHBoxLayout()
        layout.addLayout(buttons_layout)
        
        self.btn_process = QPushButton("Converter e enviar ao Gemini")
        self.btn_process.clicked.connect(self.process_pdf)
        buttons_layout.addWidget(self.btn_process)
        
        self.btn_save_json = QPushButton("Salvar JSON em arquivo")
        self.btn_save_json.clicked.connect(self.save_json)
        self.btn_save_json.setEnabled(False)  # Desabilitado até ter um JSON
        buttons_layout.addWidget(self.btn_save_json)

        # Área de texto para exibir resultado JSON ou erros
        self.text_result = QTextEdit()
        self.text_result.setReadOnly(True)
        layout.addWidget(self.text_result)

        # Instancia converter docling
        self.doc_converter = DocumentConverter()

    def select_pdf_dialog(self):
        """Abre caixa de diálogo de arquivo para selecionar PDF local."""
        pdf_path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecionar PDF",
            "",
            "Arquivos PDF (*.pdf)"
        )
        if pdf_path:
            self.line_pdf.setText(pdf_path)
            self.current_pdf_path = pdf_path

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
                QMessageBox.information(
                    self, 
                    "Sucesso", 
                    f"Arquivo JSON salvo com sucesso em:\n{json_filepath}"
                )
            else:
                QMessageBox.warning(self, "Erro", "Não foi possível salvar o arquivo JSON.")
                
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Erro", 
                f"Erro ao salvar arquivo: {str(e)}"
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

        self.current_pdf_path = pdf_source
        self.current_json_result = None
        self.raw_gemini_response = None
        self.btn_save_json.setEnabled(False)
        
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
                                    self.text_result.append(f"\n\nJSON reparado salvo em: {json_filepath}")
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
                
                # Salvar automaticamente em arquivo
                try:
                    json_filepath = save_json_to_file(json_result, pdf_source)
                    if json_filepath:
                        self.text_result.append(f"\n\nJSON salvo automaticamente em: {json_filepath}")
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