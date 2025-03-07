import json
import logging
from datetime import datetime
from typing import List, Optional

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# =========================================
# ============= DOMAIN MODELS =============
# =========================================

class OrgaoContratante:
    def __init__(self, razao_social: str, sigla: Optional[str], cnpj: str):
        self.razao_social = razao_social
        self.sigla = sigla
        self.cnpj = cnpj


class EmpresaContratada:
    def __init__(self, razao_social: str, cnpj: str):
        self.razao_social = razao_social
        self.cnpj = cnpj


class Item:
    def __init__(self,
                 descricao: str,
                 especificacao: Optional[str],
                 unidade_medida: Optional[str],
                 quantidade: Optional[str],
                 valor_unitario: Optional[str],
                 valor_total: Optional[str],
                 catmat_catser: Optional[str],
                 tipo: str,
                 locais_execucao_entrega: Optional[str]):
        self.descricao = descricao
        self.especificacao = especificacao
        self.unidade_medida = unidade_medida
        self.quantidade = quantidade
        self.valor_unitario = valor_unitario
        self.valor_total = valor_total
        self.catmat_catser = catmat_catser
        self.tipo = tipo
        self.locais_execucao_entrega = locais_execucao_entrega


class Contrato:
    def __init__(self,
                 numero_contrato: str,
                 tipo_instrumento: str,
                 processo_administrativo: Optional[str],
                 data_celebracao: Optional[str],
                 fonte_preco: str,
                 referencia_contrato: Optional[str],
                 url_pdf_s3: Optional[str],
                 status_extracao: str,
                 orgao_contratante: OrgaoContratante,
                 empresa_contratada: EmpresaContratada,
                 itens: List[Item]):
        self.numero_contrato = numero_contrato
        self.tipo_instrumento = tipo_instrumento
        self.processo_administrativo = processo_administrativo
        self.data_celebracao = data_celebracao
        self.fonte_preco = fonte_preco
        self.referencia_contrato = referencia_contrato
        self.url_pdf_s3 = url_pdf_s3
        self.status_extracao = status_extracao
        self.orgao_contratante = orgao_contratante
        self.empresa_contratada = empresa_contratada
        self.itens = itens


# =========================================
# ============= PARSER CLASS ==============
# =========================================

class ContractParser:
    """
    Responsável por converter JSON em objetos de domínio (Contrato, etc.).
    """
    @staticmethod
    def parse(json_str: str) -> Contrato:
        data = json.loads(json_str)

        oc_data = data.get("orgao_contratante") or {}
        orgao_contratante = OrgaoContratante(
            razao_social=oc_data.get("razao_social", ""),
            sigla=oc_data.get("sigla"),
            cnpj=oc_data.get("cnpj", "")
        )

        ec_data = data.get("empresa_contratada") or {}
        empresa_contratada = EmpresaContratada(
            razao_social=ec_data.get("razao_social", ""),
            cnpj=ec_data.get("cnpj", "")
        )

        itens_list = []
        for item_data in data.get("itens", []):
            item = Item(
                descricao=item_data.get("descricao", ""),
                especificacao=item_data.get("especificacao"),
                unidade_medida=item_data.get("unidade_medida"),
                quantidade=item_data.get("quantidade"),
                valor_unitario=item_data.get("valor_unitario"),
                valor_total=item_data.get("valor_total"),
                catmat_catser=item_data.get("catmat_catser"),
                tipo=item_data.get("tipo", "Material"),
                locais_execucao_entrega=item_data.get("locais_execucao_entrega")
            )
            itens_list.append(item)

        contrato = Contrato(
            numero_contrato=data.get("numero_contrato", ""),
            tipo_instrumento=data.get("tipo_instrumento", "Contrato"),
            processo_administrativo=data.get("processo_administrativo"),
            data_celebracao=data.get("data_celebracao"),
            fonte_preco=data.get("fonte_preco", "Contrato"),
            referencia_contrato=data.get("referencia_contrato"),
            url_pdf_s3=data.get("url_pdf_s3"),  # substitui anexo_contrato
            status_extracao=data.get("status_extracao", "Sucesso"),
            orgao_contratante=orgao_contratante,
            empresa_contratada=empresa_contratada,
            itens=itens_list
        )
        return contrato


# =========================================
# ========== REPOSITORY CLASS =============
# =========================================

class ContractRepository:
    """
    Classe responsável por fazer a persistência no Postgres (schema precos).
    Inclui logging e tratamento de exceções.
    """

    def __init__(self, conn):
        self.conn = conn
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_orgao_contratante_by_cnpj(self, cnpj: str) -> Optional[int]:
        sql = """
            SELECT id FROM precos.orgao_contratante
            WHERE cnpj = %s
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (cnpj,))
            row = cur.fetchone()
            return row[0] if row else None

    def get_empresa_contratada_by_cnpj(self, cnpj: str) -> Optional[int]:
        sql = """
            SELECT id FROM precos.empresa_contratada
            WHERE cnpj = %s
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (cnpj,))
            row = cur.fetchone()
            return row[0] if row else None

    def insert_orgao_contratante(self, orgao: OrgaoContratante) -> int:
        existing_id = self.get_orgao_contratante_by_cnpj(orgao.cnpj)
        if existing_id is not None:
            self.logger.debug(f"Órgão já existe (ID={existing_id}).")
            return existing_id

        sql = """
            INSERT INTO precos.orgao_contratante (razao_social, sigla, cnpj)
            VALUES (%s, %s, %s)
            RETURNING id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (orgao.razao_social, orgao.sigla, orgao.cnpj))
            new_id = cur.fetchone()[0]
            self.logger.debug(f"Órgão inserido com ID={new_id}.")
            return new_id

    def insert_empresa_contratada(self, empresa: EmpresaContratada) -> int:
        existing_id = self.get_empresa_contratada_by_cnpj(empresa.cnpj)
        if existing_id is not None:
            self.logger.debug(f"Empresa já existe (ID={existing_id}).")
            return existing_id

        sql = """
            INSERT INTO precos.empresa_contratada (razao_social, cnpj)
            VALUES (%s, %s)
            RETURNING id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (empresa.razao_social, empresa.cnpj))
            new_id = cur.fetchone()[0]
            self.logger.debug(f"Empresa inserida com ID={new_id}.")
            return new_id

    def get_contrato_by_numero_urlpdf(self,
                                      numero_contrato: str,
                                      url_pdf_s3: Optional[str]) -> Optional[int]:
        """
        Retorna o ID do contrato se já existir um registro
        com o mesmo numero_contrato + url_pdf_s3
        """
        sql = """
            SELECT id FROM precos.contratos
            WHERE numero_contrato = %s
              AND url_pdf_s3 = %s
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (numero_contrato, url_pdf_s3))
            row = cur.fetchone()
            return row[0] if row else None

    def insert_contrato(self, contrato: Contrato,
                        orgao_id: int,
                        empresa_id: int) -> int:
        existing_id = self.get_contrato_by_numero_urlpdf(
            contrato.numero_contrato,
            contrato.url_pdf_s3
        )
        if existing_id is not None:
            self.logger.debug(f"Contrato já existe (ID={existing_id}).")
            return existing_id

        data_celebracao_sql = None
        if contrato.data_celebracao:
            try:
                data_celebracao_sql = datetime.strptime(
                    contrato.data_celebracao, "%d/%m/%Y"
                ).date()
            except ValueError:
                self.logger.warning(
                    f"Data de celebração inválida: {contrato.data_celebracao}"
                )
                data_celebracao_sql = None

        sql = """
            INSERT INTO precos.contratos (
                numero_contrato,
                tipo_instrumento,
                processo_administrativo,
                data_celebracao,
                fonte_preco,
                referencia_contrato,
                url_pdf_s3,
                status_extracao,
                orgao_contratante_id,
                empresa_contratada_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                contrato.numero_contrato,
                contrato.tipo_instrumento,
                contrato.processo_administrativo,
                data_celebracao_sql,
                contrato.fonte_preco,
                contrato.referencia_contrato,
                contrato.url_pdf_s3,
                contrato.status_extracao,
                orgao_id,
                empresa_id
            ))
            new_id = cur.fetchone()[0]
            self.logger.debug(f"Contrato inserido com ID={new_id}.")
            return new_id

    def is_valid_catmat_catser(self, code: Optional[str]) -> bool:
        """
        Retorna True se o 'code' estiver em precos.catmat (codigo_item)
        OU em precos.catser (codigo_material_servico).
        """
        if not code:
            return False

        sql_catmat = "SELECT 1 FROM precos.catmat WHERE codigo_item = %s LIMIT 1"
        with self.conn.cursor() as cur:
            cur.execute(sql_catmat, (code,))
            row = cur.fetchone()
            if row:
                return True

        sql_catser = "SELECT 1 FROM precos.catser WHERE codigo_material_servico = %s LIMIT 1"
        with self.conn.cursor() as cur:
            cur.execute(sql_catser, (code,))
            row = cur.fetchone()
            if row:
                return True

        return False

    def insert_item(self, item: Item, contrato_id: int):
        sql = """
            INSERT INTO precos.itens (
                contrato_id,
                descricao,
                especificacao,
                unidade_medida,
                quantidade,
                valor_unitario,
                valor_total,
                catmat_catser,
                tipo,
                locais_execucao_entrega
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                contrato_id,
                item.descricao,
                item.especificacao,
                item.unidade_medida,
                item.quantidade,
                item.valor_unitario,
                item.valor_total,
                item.catmat_catser,
                item.tipo,
                item.locais_execucao_entrega
            ))

    def insert_log_extrator(self,
                            cnpj_orgao: str,
                            cnpj_empresa: str,
                            numero_contrato: str,
                            data_contrato: Optional[datetime.date],
                            quantidade_itens: int,
                            status_execucao: str,
                            mensagem_log: str):
        sql = """
            INSERT INTO precos.log_extrator (
                data_registro,
                cnpj_orgao,
                cnpj_empresa,
                numero_contrato,
                data_contrato,
                quantidade_itens,
                status_execucao,
                mensagem_log
            )
            VALUES (now(), %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                cnpj_orgao,
                cnpj_empresa,
                numero_contrato,
                data_contrato,
                quantidade_itens,
                status_execucao,
                mensagem_log
            ))

    def persist_contract(self, contrato: Contrato):
        """
        Regras:
        - Descartar itens cujo catmat_catser não esteja em 'precos.catmat' (codigo_item)
          nem em 'precos.catser' (codigo_material_servico).
        - Se já existir um contrato (numero_contrato + url_pdf_s3), não duplica.
        - Gera registro em precos.log_extrator no final.
        """
        self.logger.info(f"Processando contrato: {contrato.numero_contrato}")
        log_status = "Sucesso"
        log_msg = "Contrato processado com sucesso."
        data_contrato_sql = None

        if contrato.data_celebracao:
            try:
                data_contrato_sql = datetime.strptime(
                    contrato.data_celebracao, "%d/%m/%Y"
                ).date()
            except ValueError:
                data_contrato_sql = None

        valid_items = []
        for item in contrato.itens:
            if self.is_valid_catmat_catser(item.catmat_catser):
                valid_items.append(item)
            else:
                self.logger.info(
                    f"[persist_contract] Descartando item com catmat_catser inválido: {item.catmat_catser}"
                )
        contrato.itens = valid_items
        qtd_itens = len(valid_items)

        try:
            self.conn.autocommit = False

            orgao_id = self.insert_orgao_contratante(contrato.orgao_contratante)
            empresa_id = self.insert_empresa_contratada(contrato.empresa_contratada)
            contrato_id = self.insert_contrato(contrato, orgao_id, empresa_id)

            for item in contrato.itens:
                self.insert_item(item, contrato_id)

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            log_status = "Falha"
            log_msg = f"Erro ao processar contrato: {e}"
            self.logger.error(f"Falha no processamento do contrato {contrato.numero_contrato}.", exc_info=True)
        finally:
            self.conn.autocommit = True
            try:
                self.insert_log_extrator(
                    cnpj_orgao=contrato.orgao_contratante.cnpj,
                    cnpj_empresa=contrato.empresa_contratada.cnpj,
                    numero_contrato=contrato.numero_contrato,
                    data_contrato=data_contrato_sql,
                    quantidade_itens=qtd_itens,
                    status_execucao=log_status,
                    mensagem_log=log_msg
                )
            except Exception as e_log:
                self.logger.error("Falha ao registrar log do extrator.", exc_info=True)


def generate_sql_script(contrato: Contrato, filename: str) -> str:
    """
    Gera um script SQL com INSERTs (órgão, empresa, contrato, itens),
    seguindo o schema precos.
    
    Importante: Só insere o contrato se houver pelo menos um item válido.
    """

    # 1) Abre a conexão real com suas credenciais
    conn = psycopg2.connect(
        host="54.243.92.199",
        port=5433,
        database="compras_ia",
        user="compras",
        password="12345"
    )

    try:
        # 2) Cria o repositório com a conexão, para validar os itens
        repo_stub = ContractRepository(conn)

        # 3) Filtra itens válidos
        valid_items = []
        for it in contrato.itens:
            if repo_stub.is_valid_catmat_catser(it.catmat_catser):
                valid_items.append(it)
                
        # Se não houver itens válidos, retornar um comentário SQL sem comandos de inserção
        if not valid_items:
            return f"-- Contrato de {filename} não possui itens válidos. Nenhuma ação realizada."
                
        # Construir a URL do PDF baseada no nome do arquivo
        if not filename.lower().endswith('.pdf'):
            pdf_filename = f"{filename}.pdf"
        else:
            pdf_filename = filename
            
        # Definir a URL do S3 para o PDF
        url_pdf_s3 = f"s3://compras-ia-np/Contratos/{pdf_filename}"
        
        # Verificar se já existe um contrato com este URL do PDF
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM precos.contratos WHERE url_pdf_s3 = %s LIMIT 1", (url_pdf_s3,))
            existing_contract = cur.fetchone()
            
        # Se já existe um contrato com este URL do PDF, retornar um comentário indicando
        if existing_contract:
            return f"-- Contrato com URL do PDF '{url_pdf_s3}' já existe no banco de dados (ID={existing_contract[0]}). Nenhuma ação realizada."
        
        # Atualizar o contrato com a URL correta
        contrato.url_pdf_s3 = url_pdf_s3

        # 4) Monta o script
        script_lines = []

        orgao_cnpj = contrato.orgao_contratante.cnpj
        orgao_razao = (contrato.orgao_contratante.razao_social or "").replace("'", "''")
        orgao_sigla = (contrato.orgao_contratante.sigla or "").replace("'", "''")

        script_lines.append(f"""-- Inserindo Orgao Contratante
INSERT INTO precos.orgao_contratante (razao_social, sigla, cnpj)
VALUES ('{orgao_razao}', '{orgao_sigla}', '{orgao_cnpj}')
ON CONFLICT (cnpj) DO NOTHING; 
""")

        emp_cnpj = contrato.empresa_contratada.cnpj
        emp_razao = (contrato.empresa_contratada.razao_social or "").replace("'", "''")
        script_lines.append(f"""-- Inserindo Empresa Contratada
INSERT INTO precos.empresa_contratada (razao_social, cnpj)
VALUES ('{emp_razao}', '{emp_cnpj}')
ON CONFLICT (cnpj) DO NOTHING;
""")

        # Verificar se a data está em formato válido (DD/MM/YYYY)
        data_sql = "NULL"
        if contrato.data_celebracao:
            import re
            if re.match(r'^\d{2}/\d{2}/\d{4}$', contrato.data_celebracao):
                data_sql = f"TO_DATE('{contrato.data_celebracao}', 'DD/MM/YYYY')"
            else:
                # Se não estiver no formato esperado (ex: 'N/A'), usar NULL
                data_sql = "NULL"

        numero_contrato = (contrato.numero_contrato or "").replace("'", "''")
        tipo_instrumento = (contrato.tipo_instrumento or "").replace("'", "''")
        processo_adm = (contrato.processo_administrativo or "").replace("'", "''")
        fonte_preco = (contrato.fonte_preco or "").replace("'", "''")
        ref_contrato = (contrato.referencia_contrato or "").replace("'", "''")

        url_pdf_s3_value = (contrato.url_pdf_s3 or "").replace("'", "''")
        status = (contrato.status_extracao or "").replace("'", "''")

        # Usar CTEs para garantir que tudo seja inserido em uma única transação
        script_lines.append(f"""-- Inserir contrato (se não existir) e obter ID
WITH contrato_upsert AS (
    INSERT INTO precos.contratos (
        numero_contrato,
        tipo_instrumento,
        processo_administrativo,
        data_celebracao,
        fonte_preco,
        referencia_contrato,
        url_pdf_s3,
        status_extracao,
        orgao_contratante_id,
        empresa_contratada_id
    )
    SELECT 
        '{numero_contrato}',
        '{tipo_instrumento}',
        '{processo_adm}',
        {data_sql},
        '{fonte_preco}',
        '{ref_contrato}',
        '{url_pdf_s3_value}',
        '{status}',
        (SELECT id FROM precos.orgao_contratante WHERE cnpj = '{orgao_cnpj}' LIMIT 1),
        (SELECT id FROM precos.empresa_contratada WHERE cnpj = '{emp_cnpj}' LIMIT 1)
    WHERE 
        NOT EXISTS (
            SELECT 1 
            FROM precos.contratos 
            WHERE url_pdf_s3 = '{url_pdf_s3_value}'
        )
    RETURNING id
)""")

        # Obter ID do contrato (novo ou existente)
        script_lines.append(""", contrato_id AS (
    SELECT id FROM contrato_upsert
    UNION ALL
    SELECT id FROM precos.contratos 
    WHERE url_pdf_s3 = '""" + url_pdf_s3_value + """'
      AND NOT EXISTS (SELECT 1 FROM contrato_upsert)
    LIMIT 1
)""")

        # Inserir os itens
        script_lines.append("""-- Inserir itens
INSERT INTO precos.itens (
    contrato_id,
    descricao,
    especificacao,
    unidade_medida,
    quantidade,
    valor_unitario,
    valor_total,
    catmat_catser,
    tipo,
    locais_execucao_entrega
)""")

        first_item = True
        for item in valid_items:
            desc = (item.descricao or "").replace("'", "''")
            espec = (item.especificacao or "").replace("'", "''")
            um = (item.unidade_medida or "").replace("'", "''")
            qtd = (item.quantidade or "").replace("'", "")
            val_unit = (item.valor_unitario or "").replace("'", "")
            val_total = (item.valor_total or "").replace("'", "")
            catmat = (item.catmat_catser or "").replace("'", "")
            tipo = (item.tipo or "").replace("'", "''")
            locais = (item.locais_execucao_entrega or "").replace("'", "''")

            if first_item:
                script_lines.append("SELECT")
                first_item = False
            else:
                script_lines.append("UNION ALL\nSELECT")

            script_lines.append(f"""
    id,
    '{desc}',
    '{espec}',
    '{um}',
    '{qtd}',
    '{val_unit}',
    '{val_total}',
    '{catmat}',
    '{tipo}',
    '{locais}'
FROM contrato_id""")

        script_lines.append(";")

        return "\n".join(script_lines)

    finally:
        # 5) Fecha a conexão depois de gerar o script
        conn.close()