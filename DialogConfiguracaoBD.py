###############################################################################
# DialogConfiguracaoBD.py - Diálogo para configuração do banco de dados
###############################################################################
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QGridLayout, QMessageBox,
    QCheckBox, QSpinBox
)
from PyQt5.QtCore import QSettings

class DialogConfiguracaoBD(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuração do PostgreSQL")
        self.setMinimumWidth(400)
        
        # Carregar configurações salvas
        self.settings = QSettings("Docling", "GeminiExtractor")
        
        layout = QVBoxLayout()
        
        # Campos de configuração em grid
        grid = QGridLayout()
        
        # Host
        grid.addWidget(QLabel("Host:"), 0, 0)
        self.edit_host = QLineEdit(self.settings.value("db/host", "54.175.112.114"))
        grid.addWidget(self.edit_host, 0, 1)
        
        # Porta
        grid.addWidget(QLabel("Porta:"), 1, 0)
        self.spin_port = QSpinBox()
        self.spin_port.setMinimum(1)
        self.spin_port.setMaximum(65535)
        self.spin_port.setValue(int(self.settings.value("db/port", 5433)))
        grid.addWidget(self.spin_port, 1, 1)
        
        # Banco de dados
        grid.addWidget(QLabel("Banco de Dados:"), 2, 0)
        self.edit_database = QLineEdit(self.settings.value("db/database", "compras_ia"))
        grid.addWidget(self.edit_database, 2, 1)
        
        # Usuário
        grid.addWidget(QLabel("Usuário:"), 3, 0)
        self.edit_user = QLineEdit(self.settings.value("db/user", "compras"))
        grid.addWidget(self.edit_user, 3, 1)
        
        # Senha
        grid.addWidget(QLabel("Senha:"), 4, 0)
        self.edit_password = QLineEdit(self.settings.value("db/password", "Sinerji"))
        self.edit_password.setEchoMode(QLineEdit.Password)
        grid.addWidget(self.edit_password, 4, 1)
        
        layout.addLayout(grid)
        
        # Checkbox para executar SQL automaticamente após gerar
        self.check_auto_exec = QCheckBox("Executar SQL automaticamente após gerar")
        self.check_auto_exec.setChecked(self.settings.value("db/auto_exec", "true") == "true")
        layout.addWidget(self.check_auto_exec)
        
        # Botões
        button_layout = QHBoxLayout()
        
        self.btn_test = QPushButton("Testar Conexão")
        self.btn_test.clicked.connect(self.test_connection)
        button_layout.addWidget(self.btn_test)
        
        self.btn_save = QPushButton("Salvar")
        self.btn_save.clicked.connect(self.accept)
        button_layout.addWidget(self.btn_save)
        
        self.btn_cancel = QPushButton("Cancelar")
        self.btn_cancel.clicked.connect(self.reject)
        button_layout.addWidget(self.btn_cancel)
        
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
    
    def get_db_config(self):
        """Retorna as configurações atuais do diálogo"""
        return {
            "host": self.edit_host.text(),
            "port": self.spin_port.value(),
            "database": self.edit_database.text(),
            "user": self.edit_user.text(),
            "password": self.edit_password.text()
        }
    
    def test_connection(self):
        """Testa a conexão com o banco usando as configurações atuais"""
        try:
            # Importar psycopg2 diretamente aqui para evitar dependência circular
            import psycopg2
            
            # Obter configurações atuais
            config = self.get_db_config()
            
            # Tentar conectar
            connection = psycopg2.connect(
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["user"],
                password=config["password"],
                connect_timeout=5
            )
            
            # Verificar versão do PostgreSQL para confirmar conexão
            with connection.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                
            # Se conseguiu conectar, conexão está ok
            connection.close()
            
            QMessageBox.information(
                self,
                "Sucesso",
                f"Conexão com PostgreSQL estabelecida com sucesso!\nVersão: {version}"
            )
            
        except Exception as e:
            QMessageBox.critical(
                self,
                "Erro de Conexão",
                f"Não foi possível conectar ao PostgreSQL:\n{str(e)}"
            )
    
    def save_settings(self):
        """Salva as configurações do banco de dados"""
        self.settings.setValue("db/host", self.edit_host.text())
        self.settings.setValue("db/port", self.spin_port.value())
        self.settings.setValue("db/database", self.edit_database.text())
        self.settings.setValue("db/user", self.edit_user.text())
        self.settings.setValue("db/password", self.edit_password.text())
        self.settings.setValue("db/auto_exec", "true" if self.check_auto_exec.isChecked() else "false")
        self.settings.sync()