import os
import csv
import re
from datetime import datetime
import pickle
from pathlib import Path
import sys
import traceback
import pandas as pd
from tqdm import tqdm # type: ignore
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import mimetypes

from cachetools import cached, TTLCache # type: ignore
import functools

# ---- Bloco de Importação e Depuração para alive_progress ----
_alive_bar_imported_successfully = False
try:
    from alive_progress import alive_bar # type: ignore
    if 'alive_progress' in sys.modules and hasattr(sys.modules['alive_progress'], '__file__'):
        print(f"DEBUG: Caminho do módulo 'alive_progress' carregado: {os.path.abspath(sys.modules['alive_progress'].__file__)}")
    _alive_bar_imported_successfully = True
except ImportError as e_alive_bar_import_debug:
    print(f"ERRO CRÍTICO: Não foi possível importar alive_bar de alive_progress: {e_alive_bar_import_debug}")
    class alive_bar_placeholder:
        def __init__(self, total=None, title=None, unit="", *args, **kwargs):
            self.title = title if title else "Progresso..."
            self.unit = unit
            self.current = 0
            self.total = total
        def __call__(self, increment=1): self.current += increment; return self
        def text(self, s_text=None): return self
        def title(self, new_title=None):
            if new_title: self.title = new_title
            return self
        def __enter__(self): return self
        def __exit__(self, *args): pass
    alive_bar = alive_bar_placeholder
# ---- Fim Bloco alive_progress ----

try:
    from google.oauth2.credentials import Credentials # type: ignore
    import google.auth.transport.requests # type: ignore
    from google_auth_oauthlib.flow import InstalledAppFlow # type: ignore
    from googleapiclient.discovery import build # type: ignore
    from googleapiclient.errors import HttpError # type: ignore
    GOOGLE_API_LIBS_AVAILABLE = True
except ImportError:
    GOOGLE_API_LIBS_AVAILABLE = False
    print("AVISO CRÍTICO: Bibliotecas Google API não instaladas!")
try:
    import gspread # type: ignore
    import gspread.utils # type: ignore
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    print("AVISO CRÍTICO: Biblioteca gspread não instalada!")
try:
    import tkinter as tk
    from tkinter import ttk, messagebox, scrolledtext
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False
    print("AVISO CRÍTICO: Biblioteca Tkinter não encontrada!")
try:
    import xlsxwriter # type: ignore
    XLSXWRITER_AVAILABLE = True
except ImportError:
    XLSXWRITER_AVAILABLE = False
    print("AVISO: Biblioteca xlsxwriter não instalada! A geração de Excel usará openpyxl (pode ser mais lenta).")

# --- Constantes Globais ---
SCOPES_DRIVE = ['https://www.googleapis.com/auth/drive.readonly']
SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SCOPES_GMAIL_SEND = ['https://www.googleapis.com/auth/gmail.send']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_PICKLE_FILE = 'token_google_apis.pickle'
DRIVE_ROOT_FOLDER_ID = "10aj-P0-NO6gxMe-NtevM3RIQJvsO7NFg" # Substitua pelo seu ID de pasta raiz, se necessário
PROJECTS_ROOT_LOCAL_SYNC = r"G:\.shortcut-targets-by-id\10aj-P0-NO6gxMe-NtevM3RIQJvsO7NFg\1. Em execução" # Substitua pelo seu caminho local, se necessário


DISCIPLINE_SIGLA_TO_FOLDER_MAP = {
    "ARQ": "1 Arq", "CIV": "10 Civil", "EMT": "06 Estrutura Metalica", "ELE": "9 Elétr",
    "HID": "04 Hidráulica", "MEC": "7 Mec", "TUB": "12 Tub", "INC": "4 A Inc", "HVA": "8 HVA",
    "AUT": "05 Automação", "FUN": "10 Civil", "PIS": "10 Civil", "PAV": "10 Civil", "TER": "10 Civil",
    "IFRA": "2 Ifra", "TEL": "3 Tel", "INSTR": "5 Instr", "PROC": "11 Processos", "NVP": "13 Nuvens",
    "BIM": "14 BIM", "ECC": "10 Civil", "UTL": "11 Processos", "PRO": "11 Processos", "INS": "5 Instr",
}
VALID_DISCIPLINE_SIGLAS = set(DISCIPLINE_SIGLA_TO_FOLDER_MAP.keys())
FOLDER_NAME_TO_DISCIPLINE_SIGLA_MAP = {v.upper(): k for k, v in DISCIPLINE_SIGLA_TO_FOLDER_MAP.items()}
DISCIPLINE_FULLNAME_TO_SIGLA_MAP = {
    "ARQUITETURA": "ARQ", "AUTOMAÇÃO": "AUT", "AUTOMACAO": "AUT", "ELÉTRICA": "ELE", "ELETRICA": "ELE",
    "HIDRÁULICA": "HID", "HIDRAULICA": "HID", "HVAC": "HVA", "UTILIDADES": "UTL", "TERRAPLANAGEM": "TER",
    "TUBULAÇÃO": "TUB", "TUBULACAO": "TUB", "MECÂNICA": "MEC", "MECANICA": "MEC", "GERAL CIVIL": "CIV",
    "ESTRUTURA METÁLICA": "EMT", "ESTRUTURA METALICA": "EMT", "ESTRUTURA DE CONCRETO": "ECC",
    "FUNDAÇÕES E CONTENÇÕES": "FUN", "FUNDACOES E CONTENCOES": "FUN", "PISO INDUSTRIAL": "PIS",
    "PAVIMENTAÇÃO": "PAV", "PAVIMENTACAO": "PAV", "ALVENARIA ESTRUTURAL": "ALV", "PAREDES DE CONCRETO": "PCC",
    "SISTEMA DE INCÊNDIO": "INC", "SISTEMA DE INCENDIO": "INC", "IRRIGAÇÃO": "IRR", "IRRIGACAO": "IRR",
    "NUVEM DE PONTOS": "NVP", "MODELO FEDERADO": "BIM", "IMPERMEABILIZAÇÃO": "IMP", "IMPERMEABILIZACAO": "IMP"
}
DISCIPLINES_AND_SUBDISCIPLINES = {
    "ELE": ["DIA", "TOM", "ILU", "PDA", "SDA", "RCA", "DET", "MDD", "MDC", "LCA", "LAY", "LDM", "BIM", "TEL"],
    "HID": ["AFP", "APL", "DRE", "AQP", "ESG", "BIM"], "HVA": ["DES", "COR", "DET", "MDC", "MDE", "LDE", "BIM"],
    "UTL": ["FLU", "ISO", "DES", "PLM", "SUP", "ESP"], "TER": ["PLC", "SEC", "DET", "LDM", "MDE", "MDC", "BIM"],
    "TUB": ["ISO", "DES", "PLM", "SUP", "ESP"], "MEC": ["PLC", "FLU", "PLM", "DES", "SUP", "MEM", "BIM"],
    "ARQ": ["VIS", "PRE", "COB", "IMP", "PLP", "PLF", "PLA", "DET", "COR", "ELE", "PAI", "MDD", "PQT", "LDE", "BIM", "NVP"],
    "EMT": ["PLC", "PLS", "DET", "COR", "ELE", "GER", "LDM", "MDE", "MDC", "BIM"],
    "ECC": ["PLC", "PFO", "COR", "DTV", "DTL", "DTP", "DTF", "DPR", "DET", "GER", "LDM", "MDE", "MDC", "BIM"],
    "FUN": ["PLC", "DET", "GER", "LDM", "MDE", "MDC", "BIM"], "PIS": ["PIS", "DET", "LDM", "MDE", "MDC", "BIM"],
    "PAV": ["PLA", "DET", "LDM", "MDE", "MDC", "BIM"], "ALV": ["PLA", "PAR", "DET", "GER", "LDM", "MDE", "MDC", "BIM"],
    "PCC": ["PLA", "PAR", "DET", "GER", "LDM", "MDE", "MDC", "BIM"], "CIV": ["BIM"],
    "INC": ["EQU", "SPK", "MDC", "MDE", "LDE", "BIM"], "IRR": ["DES", "PQT", "ESP"], "NVP": ["MOD"], "BIM": ["FDR"],
    "IMP": ["DES", "MDD", "PQT"], "AUT": ["ARA", "TOP", "RRE", "RCR", "SEG", "RCS", "FLU", "DES", "DET", "MDD", "LDE", "LDM", "BIM"],
    "TEL": [], "INSTR": [], "PROC": [], "INS": []
}

TEMP_DOWNLOAD_DIR = "temp_ld_downloads"
LD_SHEET_FILENAME_COLUMN_LETTER = 'E'
LD_SHEET_DISCIPLINA_LD_COLUMN_LETTER = 'I'
LD_SHEET_REVISAO_ATUAL_COL_G_LETTER = 'G'
LD_SHEET_STATUS_COL_O_LETTER = 'O' 
LD_SHEET_SITUACAO_COLUMN_LETTER = 'H'
LD_SHEET_SUBDISCIPLINA_LD_COLUMN_LETTER = 'J'
LD_SHEET_START_DATA_ROW = 15 

ALLOWED_EXTENSIONS_DRIVE_SEARCH = ["pdf", "dwg", "ifc", "nwd"]

DRIVE_CACHE = TTLCache(maxsize=200, ttl=3600)
PAGE_SIZE = 1000

drive_service = None
gspread_client = None
gmail_service = None
all_project_folders_local_sorted = []
app_gui_instance = None
new_col_A_name = "Nome do arquivo" 

# ==============================================================================
# DEFINIÇÃO DA CLASSE ProjectSelectorApp (GUI)
# ==============================================================================
class ProjectSelectorApp:
    def __init__(self, master, project_list, diverge_callback, filter_callback):
        self.master = master
        self.master.title("Gerenciador de Projetos e Verificador de Divergências v0.4") 
        self.master.geometry("850x750")

        self.project_list_full = ["TODOS OS PROJETOS"] + project_list
        self.divergence_process_callback = diverge_callback
        self.load_ld_filters_callback = filter_callback

        self.generated_reports_paths = {"divergence": [], "summary": []}
        self.all_filter_options_from_ld = {}

        style = ttk.Style()
        style.theme_use('clam')

        main_frame = ttk.Frame(master, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        project_frame = ttk.LabelFrame(main_frame, text="Seleção de Projeto", padding="10")
        project_frame.pack(fill=tk.X, pady=(0,10))

        ttk.Label(project_frame, text="Projeto:").pack(side=tk.LEFT, padx=(0,5))
        self.selected_project_var = tk.StringVar()
        self.project_combobox = ttk.Combobox(project_frame, textvariable=self.selected_project_var, state="readonly", width=70)
        self.project_combobox['values'] = self.project_list_full
        if self.project_list_full:
            self.project_combobox.set(self.project_list_full[0])
        self.project_combobox.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.project_combobox.bind("<<ComboboxSelected>>", self.on_project_selection_change)

        filters_frame = ttk.LabelFrame(main_frame, text="Filtros da LD (para Relatórios)", padding="10")
        filters_frame.pack(fill=tk.X, pady=5)

        ttk.Label(filters_frame, text="Disciplina (LD):").grid(row=0, column=0, padx=5, pady=3, sticky=tk.W)
        self.filter_disciplina_var = tk.StringVar()
        self.disciplina_combobox = ttk.Combobox(filters_frame, textvariable=self.filter_disciplina_var, state="readonly", width=28)
        self.disciplina_combobox.grid(row=0, column=1, padx=5, pady=3, sticky=tk.EW)
        self.disciplina_combobox.bind("<<ComboboxSelected>>", self.on_disciplina_filter_change)

        ttk.Label(filters_frame, text="Subdisciplina (LD):").grid(row=0, column=2, padx=5, pady=3, sticky=tk.W)
        self.filter_subdisciplina_var = tk.StringVar()
        self.subdisciplina_combobox = ttk.Combobox(filters_frame, textvariable=self.filter_subdisciplina_var, state="readonly", width=28)
        self.subdisciplina_combobox.grid(row=0, column=3, padx=5, pady=3, sticky=tk.EW)
        self.subdisciplina_combobox.bind("<<ComboboxSelected>>", self.on_subdisciplina_filter_change)

        ttk.Label(filters_frame, text="Situação (LD):").grid(row=1, column=0, padx=5, pady=3, sticky=tk.W)
        self.filter_situacao_var = tk.StringVar()
        self.situacao_combobox = ttk.Combobox(filters_frame, textvariable=self.filter_situacao_var, state="readonly", width=28)
        self.situacao_combobox.grid(row=1, column=1, padx=5, pady=3, sticky=tk.EW)

        self.clear_filters_button = ttk.Button(filters_frame, text="Limpar Filtros", command=self.clear_all_filters)
        self.clear_filters_button.grid(row=1, column=3, padx=5, pady=8, sticky=tk.E)

        filters_frame.columnconfigure(1, weight=1)
        filters_frame.columnconfigure(3, weight=1)

        actions_frame = ttk.LabelFrame(main_frame, text="Ações", padding="10")
        actions_frame.pack(fill=tk.X, pady=10)

        self.generate_divergence_button = ttk.Button(actions_frame, text="Gerar Rel. Divergências", command=self.run_divergence_process)
        self.generate_divergence_button.grid(row=0, column=0, padx=5, pady=5, sticky=tk.EW)

        self.generate_summary_button = ttk.Button(actions_frame, text="Gerar Resumo da LD", command=self.run_summary_ld_process)
        self.generate_summary_button.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)

        self.clear_cache_button = ttk.Button(actions_frame, text="Limpar Cache Drive", command=self.confirm_clear_drive_cache)
        self.clear_cache_button.grid(row=0, column=2, padx=5, pady=5, sticky=tk.EW)

        actions_frame.columnconfigure(0, weight=1)
        actions_frame.columnconfigure(1, weight=1)
        actions_frame.columnconfigure(2, weight=1)

        log_frame = ttk.LabelFrame(main_frame, text="Logs e Mensagens", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(5,0))

        self.log_text_widget = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=15, state=tk.DISABLED, relief=tk.SOLID, borderwidth=1)
        self.log_text_widget.pack(fill=tk.BOTH, expand=True)

        self.progress_label_var = tk.StringVar()
        self.progress_label = ttk.Label(main_frame, textvariable=self.progress_label_var)
        self.progress_label.pack(fill=tk.X, pady=(5,0))
        self.progressbar = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, length=100, mode='determinate')
        self.progressbar.pack(fill=tk.X, pady=(0,5))

        self.initial_filter_load()

    def log_message_to_gui_text_widget(self, message):
        if self.log_text_widget and hasattr(self.master, 'winfo_exists') and self.master.winfo_exists():
            try:
                self.log_text_widget.config(state=tk.NORMAL)
                self.log_text_widget.insert(tk.END, f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
                self.log_text_widget.see(tk.END)
                self.log_text_widget.config(state=tk.DISABLED)
            except tk.TclError:
                pass

    def initial_filter_load(self):
        selected_proj = self.selected_project_var.get()
        if selected_proj and selected_proj != "TODOS OS PROJETOS":
             self.log_message_to_gui_text_widget(f"INFO: Carregando opções de filtro iniciais para {selected_proj}.")
             self.update_progress_display(text="Carregando filtros da LD...")
             self.load_ld_filters_callback(selected_proj, self)
        else:
            self.update_filter_options_display({})

    def on_project_selection_change(self, event=None):
        selected_proj = self.selected_project_var.get()
        self.log_message_to_gui_text_widget(f"Projeto selecionado: {selected_proj}")
        if selected_proj and selected_proj != "TODOS OS PROJETOS":
            self.update_progress_display(text=f"Carregando filtros para {selected_proj[:30]}...")
            self.load_ld_filters_callback(selected_proj, self)
        else:
            self.update_filter_options_display({})

    def get_current_filters(self):
        filters = {
            "disciplina": self.filter_disciplina_var.get() if self.filter_disciplina_var.get() and self.filter_disciplina_var.get() != "TODAS" else None,
            "subdisciplina": self.filter_subdisciplina_var.get() if self.filter_subdisciplina_var.get() and self.filter_subdisciplina_var.get() != "TODAS" else None,
            "situacao": self.filter_situacao_var.get() if self.filter_situacao_var.get() and self.filter_situacao_var.get() != "TODAS" else None,
        }
        return {k: v for k, v in filters.items() if v is not None}

    def run_divergence_process(self):
        selected_project = self.selected_project_var.get()
        if not selected_project:
            messagebox.showwarning("Seleção Necessária", "Por favor, selecione um projeto.")
            return
        current_filters = self.get_current_filters()
        self.log_message_to_gui_text_widget(f"INFO: Iniciando processo de divergências para '{selected_project}' com filtros: {current_filters if current_filters else 'Nenhum'}")
        self.disable_action_buttons()
        self.update_progress_display(text=f"Processando divergências para {selected_project[:30]}...", value=0, mode='indeterminate')
        self.divergence_process_callback(selected_project, current_filters, self)

    def run_summary_ld_process(self):
        selected_project = self.selected_project_var.get()
        if not selected_project or selected_project == "TODOS OS PROJETOS":
            messagebox.showwarning("Seleção Necessária", "Por favor, selecione um projeto específico para gerar o resumo da LD.")
            return
        current_filters = self.get_current_filters()
        self.log_message_to_gui_text_widget(f"INFO: Iniciando geração de resumo da LD para '{selected_project}' com filtros: {current_filters if current_filters else 'Nenhum'}")
        self.disable_action_buttons()
        self.update_progress_display(text=f"Gerando resumo LD para {selected_project[:30]}...", value=0, mode='indeterminate')
        threading.Thread(target=self._execute_summary_ld_process,
                         args=(selected_project, current_filters),
                         daemon=True).start()

    def _execute_summary_ld_process(self, project_name_for_summary, current_filters):
        global drive_service, gspread_client
        project_drive_id = get_folder_id_from_path(drive_service, DRIVE_ROOT_FOLDER_ID, project_name_for_summary)
        if not project_drive_id:
            self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"ERRO: Pasta do projeto '{project_name_for_summary}' não encontrada no Drive para resumo."))
            self.master.after(0, self.reenable_action_buttons)
            self.master.after(0, lambda: self.update_progress_display(text="Falha ao encontrar pasta no Drive.", mode='determinate'))
            return

        project_code_part = project_name_for_summary.split(' - ', 1)[0]
        cp_search_match = re.match(r"([A-Z_]{0,4})?(\d{2}[_.]?\d+)", project_code_part)
        if cp_search_match:
            prefix = cp_search_match.group(1) or ""
            core_cp = cp_search_match.group(2)
            normalized_core_cp = core_cp.replace('_', '.')
            if len(prefix) > 0 and prefix.endswith('_'):
                prefix = prefix[:-1]
            cp_search = f"{prefix}{normalized_core_cp}"
        else:
            cp_search = project_code_part.replace('_', '.')
            log_message(f"AVISO (_execute_summary_ld_process): Regex CP não casou com '{project_code_part}'. Usando fallback: '{cp_search}'")
        if not cp_search:
            log_message(f"ERRO FATAL (_execute_summary_ld_process): Não foi possível extrair CP de '{project_name_for_summary}'.")
            self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"ERRO: Falha ao derivar CP de '{project_name_for_summary}'."))
            self.master.after(0, self.reenable_action_buttons)
            self.master.after(0, lambda: self.update_progress_display(text="Erro ao derivar CP.", mode='determinate'))
            return

        ld_drive_files = find_project_ld_file_drive(drive_service, project_drive_id, cp_search)
        raw_docs_ld_for_summary = []
        if ld_drive_files:
            ld_fn, ld_id, ld_mime = ld_drive_files[0]
            self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"INFO: Lendo LD '{ld_fn}' para resumo... (CP Buscado: {cp_search})"))
            if ld_mime == 'application/vnd.google-apps.spreadsheet':
                raw_docs_ld_for_summary = read_ld_google_sheet_with_creds(gspread_client, ld_id, ld_fn)
            elif ld_fn.lower().endswith(".csv") and any(m in ld_mime for m in ['text/csv', 'excel', 'octet-stream']):
                temp_dir = Path(TEMP_DOWNLOAD_DIR); temp_dir.mkdir(parents=True, exist_ok=True)
                safe_proj_name_dl = "".join(c if c.isalnum() else "_" for c in project_name_for_summary)
                dl_path_summary = temp_dir / f"TEMP_SUMMARY_{safe_proj_name_dl}_{Path(ld_fn).name}"
                try:
                    req = drive_service.files().get_media(fileId=ld_id)
                    with open(dl_path_summary, 'wb') as f: f.write(req.execute())
                    raw_docs_ld_for_summary = read_ld_csv_file(str(dl_path_summary))
                except Exception as e_dl_sum:
                    self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"ERRO ao baixar/ler CSV LD para resumo: {e_dl_sum}"))
                finally:
                    if dl_path_summary.exists(): dl_path_summary.unlink(missing_ok=True)
        else:
            self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"AVISO: Nenhuma LD encontrada para '{project_name_for_summary}' (CP Buscado: {cp_search}) para resumo."))
            self.master.after(0, self.reenable_action_buttons); self.master.after(0, lambda: self.update_progress_display(text="Nenhuma LD encontrada.", mode='determinate'))
            return
        if not raw_docs_ld_for_summary:
            self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"Nenhum item na LD de '{project_name_for_summary}' para resumir."))
            self.master.after(0, self.reenable_action_buttons); self.master.after(0, lambda: self.update_progress_display(text="LD vazia ou sem dados.", mode='determinate'))
            return
        report_file = generate_ld_summary_report(project_name_for_summary, raw_docs_ld_for_summary, current_filters)
        if report_file:
            self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"SUCESSO: Resumo da LD gerado: {report_file}"))
            self.master.after(0, lambda: messagebox.showinfo("Resumo Gerado", f"Relatório de resumo da LD salvo em:\n{report_file}"))
            self.set_generated_reports([report_file], "summary")
            self.master.after(0, lambda: self.update_progress_display(text="Resumo LD gerado!", mode='determinate', value=100))
        else:
            self.master.after(0, lambda: self.log_message_to_gui_text_widget(f"FALHA: Não foi possível gerar o resumo da LD para '{project_name_for_summary}'."))
            self.master.after(0, lambda: self.update_progress_display(text="Falha ao gerar resumo LD.", mode='determinate'))
        self.master.after(0, self.reenable_action_buttons)

    def confirm_clear_drive_cache(self):
        if messagebox.askyesno("Confirmar Limpeza de Cache", "Tem certeza que deseja limpar o cache de listagem de arquivos do Google Drive?\nIsso pode tornar a próxima varredura do Drive mais lenta."):
            clear_drive_cache(); self.log_message_to_gui_text_widget("INFO: Cache do Drive limpo.")
            messagebox.showinfo("Cache Limpo", "O cache de listagem do Google Drive foi limpo com sucesso.")

    def disable_action_buttons(self):
        self.generate_divergence_button.config(state=tk.DISABLED); self.generate_summary_button.config(state=tk.DISABLED)
        self.clear_cache_button.config(state=tk.DISABLED); self.project_combobox.config(state=tk.DISABLED)
        self.clear_filters_button.config(state=tk.DISABLED); self.disciplina_combobox.config(state=tk.DISABLED)
        self.subdisciplina_combobox.config(state=tk.DISABLED); self.situacao_combobox.config(state=tk.DISABLED)

    def reenable_action_buttons(self):
        if hasattr(self.master, 'winfo_exists') and self.master.winfo_exists():
            self.generate_divergence_button.config(state=tk.NORMAL); self.generate_summary_button.config(state=tk.NORMAL)
            self.clear_cache_button.config(state=tk.NORMAL); self.project_combobox.config(state="readonly")
            self.clear_filters_button.config(state=tk.NORMAL); self.disciplina_combobox.config(state="readonly")
            self.subdisciplina_combobox.config(state="readonly"); self.situacao_combobox.config(state="readonly")
            self.update_progress_display(text="Pronto.", value=0, mode='determinate')

    def reenable_generate_button(self): self.reenable_action_buttons()

    def update_filter_options_display(self, filter_data_map):
        self.all_filter_options_from_ld = filter_data_map
        disciplinas = ["TODAS"] + sorted(filter_data_map.get('disciplinas_list_for_gui', []))
        self.disciplina_combobox['values'] = disciplinas
        if disciplinas: self.filter_disciplina_var.set(disciplinas[0])
        self.on_disciplina_filter_change()
        self.update_progress_display(text="Filtros da LD carregados." if filter_data_map else "Filtros da LD limpos.", mode='determinate', value=100 if filter_data_map else 0)

    def on_disciplina_filter_change(self, event=None):
        selected_disciplina = self.filter_disciplina_var.get()
        subdisciplinas = ["TODAS"]
        if selected_disciplina and selected_disciplina != "TODAS":
            subdisciplinas.extend(sorted(self.all_filter_options_from_ld.get('subdisciplinas_by_discipline_map', {}).get(selected_disciplina, [])))
        else: subdisciplinas.extend(sorted(self.all_filter_options_from_ld.get('all_subdisciplinas_list_for_gui', [])))
        self.subdisciplina_combobox['values'] = subdisciplinas
        if subdisciplinas: self.filter_subdisciplina_var.set(subdisciplinas[0])
        self.on_subdisciplina_filter_change()

    def on_subdisciplina_filter_change(self, event=None):
        selected_disciplina = self.filter_disciplina_var.get(); selected_subdisciplina = self.filter_subdisciplina_var.get()
        situacoes = ["TODAS"]
        if selected_disciplina and selected_disciplina != "TODAS":
            if selected_subdisciplina and selected_subdisciplina != "TODAS":
                situacoes.extend(sorted(self.all_filter_options_from_ld.get('situacoes_by_discipline_and_subdiscipline_map', {}).get(selected_disciplina, {}).get(selected_subdisciplina, [])))
            else: situacoes.extend(sorted(self.all_filter_options_from_ld.get('situacoes_by_discipline_map', {}).get(selected_disciplina, [])))
        else: situacoes.extend(sorted(self.all_filter_options_from_ld.get('all_situacoes_list_for_gui', [])))
        self.situacao_combobox['values'] = situacoes
        if situacoes: self.filter_situacao_var.set(situacoes[0])

    def clear_all_filters(self):
        self.filter_disciplina_var.set("TODAS"); self.on_disciplina_filter_change()
        self.log_message_to_gui_text_widget("INFO: Filtros da LD foram limpos.")

    def update_project_progress(self, value=None, total=None, text=None, mode=None):
        if hasattr(self.master, 'winfo_exists') and self.master.winfo_exists():
            if text: self.progress_label_var.set(text)
            current_mode = self.progressbar['mode']
            if mode:
                if mode == 'indeterminate' and current_mode != 'indeterminate': self.progressbar.config(mode='indeterminate'); self.progressbar.start(10)
                elif mode == 'determinate' and current_mode != 'indeterminate': self.progressbar.stop(); self.progressbar.config(mode='determinate')
            if self.progressbar['mode'] == 'determinate':
                if total is not None and total > 0: self.progressbar['maximum'] = total
                if value is not None: self.progressbar['value'] = value
                elif total is None and value is None: self.progressbar['value'] = 0; self.progressbar['maximum'] = 100
            try: self.master.update_idletasks()
            except tk.TclError: pass

    def set_generated_reports(self, report_paths_list, report_type_str):
        if report_type_str == "divergence": self.generated_reports_paths["divergence"].extend(report_paths_list)
        elif report_type_str == "summary": self.generated_reports_paths["summary"].extend(report_paths_list)

    def update_progress_display(self, text=None, value=None, mode='determinate'):
        if text: self.progress_label_var.set(text)
        if mode == 'indeterminate':
            if self.progressbar['mode'] != 'indeterminate': self.progressbar.config(mode='indeterminate'); self.progressbar.start(10)
        else:
            if self.progressbar['mode'] == 'indeterminate': self.progressbar.stop()
            self.progressbar.config(mode='determinate')
            if value is not None: self.progressbar['value'] = value
            else: self.progressbar['value'] = self.progressbar['value'] if value is None else 0

# ==============================================================================
# FUNÇÕES DE BACKEND E LÓGICA PRINCIPAL
# ==============================================================================
def log_message(message_text):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    formatted_message = f"{timestamp} - {message_text}"
    print(formatted_message)
    if app_gui_instance and app_gui_instance.master:
        try:
            if hasattr(app_gui_instance.master, 'winfo_exists') and app_gui_instance.master.winfo_exists():
                app_gui_instance.master.after(0, app_gui_instance.log_message_to_gui_text_widget, message_text)
        except tk.TclError as e:
            if "application has been destroyed" not in str(e).lower():
                print(f"DEBUG: Tkinter TclError during logging: {e}")
        except Exception as e_generic:
            print(f"DEBUG: Exceção ao tentar logar na GUI: {e_generic}")

def list_folders(root_path):
    excluded_folders = ["00_xxx Cliente e nome do projeto (modelo)", "html", "$RECYCLE.BIN", "System Volume Information", TEMP_DOWNLOAD_DIR]
    if not os.path.exists(root_path): log_message(f"ERRO: Caminho local '{root_path}' não existe."); return []
    if not os.path.isdir(root_path): log_message(f"ERRO: Caminho local '{root_path}' não é um diretório."); return []
    folders = []
    try:
        for entry in os.listdir(root_path):
            full_path = os.path.join(root_path, entry)
            if os.path.isdir(full_path) and entry not in excluded_folders and not entry.startswith('.'): folders.append(entry)
    except Exception as e: log_message(f"ERRO ao listar pastas locais em '{root_path}': {e}"); return []
    return folders

def get_sort_key_for_project(project_name_str):
    parts_hyphen = project_name_str.split(" - ", 1)
    if len(parts_hyphen) > 1: return parts_hyphen[1].strip().lower()
    parts_space = project_name_str.split(" ", 1)
    if len(parts_space) > 1: return parts_space[1].strip().lower()
    return project_name_str.strip().lower()

def get_google_creds(token_file_pickle, credentials_json_file, scopes_list):
    if not GOOGLE_API_LIBS_AVAILABLE: return None
    creds = None
    if os.path.exists(token_file_pickle):
        try:
            with open(token_file_pickle, 'rb') as token: creds = pickle.load(token)
            if not set(scopes_list).issubset(set(creds.scopes or [])): log_message("INFO: Escopos mudaram. Reautenticação."); creds = None
        except Exception as e: log_message(f"AVISO: Erro ao carregar token '{token_file_pickle}': {e}. Reautenticando."); creds = None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                log_message("INFO: Token expirado, atualizando...");
                if 'google.auth.transport.requests' not in sys.modules: import google.auth.transport.requests # type: ignore
                creds.refresh(google.auth.transport.requests.Request())
                if not set(scopes_list).issubset(set(creds.scopes or [])): log_message("INFO: Escopos insuficientes. Reautenticação."); creds = None
                else: log_message("INFO: Token atualizado.")
            except Exception as e_refresh: log_message(f"AVISO: Falha ao atualizar token: {e_refresh}. Reautenticação."); creds = None
        if not creds:
            if not os.path.exists(credentials_json_file):
                msg = f"ERRO FATAL: '{credentials_json_file}' não encontrado."
                log_message(msg)
                if TKINTER_AVAILABLE and app_gui_instance: app_gui_instance.master.after(0, lambda: messagebox.showerror("Erro Credenciais", msg))
                elif TKINTER_AVAILABLE: root_err = tk.Tk(); root_err.withdraw(); messagebox.showerror("Erro Credenciais", msg); root_err.destroy()
                return None
            try:
                log_message(f"INFO: Novo fluxo de autenticação: {scopes_list}"); flow = InstalledAppFlow.from_client_secrets_file(credentials_json_file, scopes_list)
                creds = flow.run_local_server(port=0); log_message("INFO: Autenticação OK.")
            except Exception as e_flow:
                msg = f"ERRO fluxo de autenticação: {e_flow}"; log_message(msg)
                if TKINTER_AVAILABLE and app_gui_instance: app_gui_instance.master.after(0, lambda: messagebox.showerror("Erro Autenticação", msg))
                elif TKINTER_AVAILABLE: root_err = tk.Tk(); root_err.withdraw(); messagebox.showerror("Erro Autenticação", msg); root_err.destroy()
                return None
        try:
            with open(token_file_pickle, 'wb') as token: pickle.dump(creds, token)
            log_message(f"INFO: Token salvo em '{token_file_pickle}'.")
        except Exception as e_pickle_save: log_message(f"AVISO: Não salvou token '{token_file_pickle}': {e_pickle_save}")
    return creds

@cached(DRIVE_CACHE)
def get_folder_id_from_path(drive_service_instance, base_folder_id, relative_folder_path_str):
    if not GOOGLE_API_LIBS_AVAILABLE: return None
    current_folder_id = base_folder_id; normalized_path = os.path.normpath(relative_folder_path_str)
    path_parts = Path(normalized_path).parts
    actual_parts_to_search = [part for part in path_parts if part and part != '.' and part != os.sep]
    if not actual_parts_to_search and relative_folder_path_str: actual_parts_to_search = [relative_folder_path_str]
    if not actual_parts_to_search: return current_folder_id
    for i, part_name in enumerate(actual_parts_to_search):
        query = f"'{current_folder_id}' in parents and name = '{part_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        try:
            response = drive_service_instance.files().list(q=query, fields="files(id, name)", pageSize=1, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            folders_found = response.get('files', [])
            if not folders_found: log_message(f"ERRO (get_folder_id): Pasta '{part_name}' NÃO encontrada em ID '{current_folder_id}'."); return None
            current_folder_id = folders_found[0]['id']
        except HttpError as error: log_message(f"ERRO API ao buscar '{part_name}' (pai: '{current_folder_id}'): {error}"); return None
        except Exception as e_gen: log_message(f"ERRO inesperado em get_folder_id buscando '{part_name}': {e_gen}"); return None
    return current_folder_id

def find_project_ld_file_drive(drive_service_instance, project_folder_drive_id, cp_base_search_term):
    if not GOOGLE_API_LIBS_AVAILABLE: return []
    log_message(f"DEBUG (find_project_ld_file_drive): Iniciando busca de LD para CP base '{cp_base_search_term}' na pasta Drive ID '{project_folder_drive_id}'")

    search_terms = [cp_base_search_term]
    prefix_match = re.match(r"^([A-Z])_?(\d.*)$", cp_base_search_term)
    if prefix_match:
        variant_cp = prefix_match.group(2)
        if variant_cp not in search_terms:
            search_terms.append(variant_cp)
            log_message(f"DEBUG (find_project_ld_file_drive): Adicionando termo de busca variante para CP: '{variant_cp}'")

    all_found_ld_files_meta = []

    for cp_term_to_use in search_terms:
        log_message(f"DEBUG (find_project_ld_file_drive): Tentando com termo CP: '{cp_term_to_use}'")
        gsheet_exact_query = f"'{project_folder_drive_id}' in parents and name = '{cp_term_to_use}.LD' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
        try:
            response = drive_service_instance.files().list(q=gsheet_exact_query, fields='files(id, name, mimeType)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            for file_item in response.get('files', []):
                if file_item.get('name', '').lower() == f"{cp_term_to_use}.LD".lower():
                    log_message(f"INFO: LD GSheet exata encontrada: {file_item['name']} (com termo '{cp_term_to_use}')")
                    all_found_ld_files_meta.append({'file': file_item, 'priority': 1, 'term': cp_term_to_use})
        except HttpError as error: log_message(f"AVISO: Erro busca LD GSheet exata ({cp_term_to_use}.LD): {error}")

        csv_exact_query = f"'{project_folder_drive_id}' in parents and name = '{cp_term_to_use}.LD.csv' and (mimeType = 'text/csv' or mimeType = 'application/vnd.ms-excel' or mimeType = 'application/octet-stream') and trashed = false"
        try:
            response = drive_service_instance.files().list(q=csv_exact_query, fields='files(id, name, mimeType)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            for file_item in response.get('files', []):
                if file_item.get('name', '').lower() == f"{cp_term_to_use}.LD.csv".lower():
                    log_message(f"INFO: LD CSV exato encontrado: {file_item['name']} (com termo '{cp_term_to_use}')")
                    all_found_ld_files_meta.append({'file': file_item, 'priority': 2, 'term': cp_term_to_use})
        except HttpError as error: log_message(f"AVISO: Erro busca LD CSV exato ({cp_term_to_use}.LD.csv): {error}")

    if all_found_ld_files_meta:
        all_found_ld_files_meta.sort(key=lambda x: (x['term'] != cp_base_search_term, x['priority']))
        unique_files_by_id = {}
        for item_meta in all_found_ld_files_meta:
            file_id = item_meta['file']['id']
            if file_id not in unique_files_by_id:
                unique_files_by_id[file_id] = item_meta
            else:
                current_best = unique_files_by_id[file_id]
                if (item_meta['term'] == cp_base_search_term and current_best['term'] != cp_base_search_term) or \
                   (item_meta['term'] == current_best['term'] and item_meta['priority'] < current_best['priority']):
                    unique_files_by_id[file_id] = item_meta
        sorted_unique_files = sorted(unique_files_by_id.values(), key=lambda x: (x['term'] != cp_base_search_term, x['priority']))
        if sorted_unique_files:
            best_match_file_item = sorted_unique_files[0]['file']
            log_message(f"DEBUG (find_project_ld_file_drive): Melhor correspondência exata selecionada: {best_match_file_item['name']}")
            return [(best_match_file_item['name'], best_match_file_item['id'], best_match_file_item.get('mimeType'))]

    log_message(f"DEBUG (find_project_ld_file_drive): Nenhuma LD exata encontrada com os termos {search_terms}. Tentando busca ampla.")
    broad_found_files_meta = []
    for cp_term_to_use in search_terms:
        gsheet_broad_query = f"'{project_folder_drive_id}' in parents and name contains '{cp_term_to_use}' and name contains 'LD' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
        try:
            response = drive_service_instance.files().list(q=gsheet_broad_query, fields='files(id, name, mimeType)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            for file_item in response.get('files', []):
                 log_message(f"INFO: LD GSheet ampla encontrada: {file_item['name']} (com termo '{cp_term_to_use}')")
                 broad_found_files_meta.append({'file': file_item, 'priority': 3, 'term': cp_term_to_use})
        except HttpError as error: log_message(f"AVISO: Erro busca ampla LD GSheet (termo '{cp_term_to_use}'): {error}")

        csv_broad_query = f"'{project_folder_drive_id}' in parents and name contains '{cp_term_to_use}' and name contains 'LD' and name contains '.csv' and (mimeType = 'text/csv' or mimeType = 'application/vnd.ms-excel' or mimeType = 'application/octet-stream') and trashed = false"
        try:
            response = drive_service_instance.files().list(q=csv_broad_query, fields='files(id, name, mimeType)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            for file_item in response.get('files', []):
                log_message(f"INFO: LD CSV amplo encontrado: {file_item['name']} (com termo '{cp_term_to_use}')")
                broad_found_files_meta.append({'file': file_item, 'priority': 4, 'term': cp_term_to_use})
        except HttpError as error: log_message(f"AVISO: Erro busca ampla LD CSV (termo '{cp_term_to_use}'): {error}")

    if broad_found_files_meta:
        broad_found_files_meta.sort(key=lambda x: (x['term'] != cp_base_search_term, x['priority'], x['file']['name']))
        unique_broad_files_dict = {}
        for item_meta in broad_found_files_meta:
            file_id = item_meta['file']['id']
            if file_id not in unique_broad_files_dict or \
               (item_meta['term'] == cp_base_search_term and unique_broad_files_dict[file_id]['term'] != cp_base_search_term) or \
               item_meta['priority'] < unique_broad_files_dict[file_id]['priority']:
                unique_broad_files_dict[file_id] = item_meta
        sorted_unique_broad_files_list = sorted(unique_broad_files_dict.values(), key=lambda x: (x['term'] != cp_base_search_term, x['priority'], x['file']['name']))
        if sorted_unique_broad_files_list:
            best_match_file_item = sorted_unique_broad_files_list[0]['file']
            log_message(f"DEBUG (find_project_ld_file_drive): Melhor correspondência ampla selecionada: {best_match_file_item['name']}")
            return [(best_match_file_item['name'], best_match_file_item['id'], best_match_file_item.get('mimeType'))]

    log_message(f"INFO: Nenhuma LD (GSheet ou CSV) encontrada para CP base '{cp_base_search_term}' (e variantes como {search_terms}) na pasta Drive ID '{project_folder_drive_id}'.")
    return []


def extract_siglas_from_filename(filename_str):
    parts = str(filename_str).split('.');
    if len(parts) >= 6:
        dis_sigla_candidate = parts[4].upper()
        if len(dis_sigla_candidate) == 3 and dis_sigla_candidate.isalnum() and dis_sigla_candidate in VALID_DISCIPLINE_SIGLAS:
            return dis_sigla_candidate
    return None


def parse_file_name(file_name: str, year_folder: str, cp_folder_numeric: str) -> dict:
    current_file_name_str = str(file_name)
    name_part_for_regex, file_extension = os.path.splitext(current_file_name_str)
    file_pattern_std1 = r"^(\d{2})\.(\d{4})\.(\d{2})\.([A-Z]{2,3})\.([A-Z]{3})\.(\d{3})\.([A-Z]{3})\.R(\d{2})$"
    file_pattern_std2 = r"^(\d{2})\.(\d{4})\.(\d{2})\.([A-Z]{2,3})\.([A-Z]{3})\.(\d{3})\.([A-Z]{3})\.([A-Z0-9]{3})\.R(\d{2})$"
    file_pattern_std3 = r"^(\d{2})\.(\d{4})\.(\d{2})\.([A-Z]{2,3})\.([A-Z]{3})\.(\d{3})\.([A-Z]{3})$"

    match_std1 = re.match(file_pattern_std1, name_part_for_regex)
    match_std2 = re.match(file_pattern_std2, name_part_for_regex)
    match_std3 = re.match(file_pattern_std3, name_part_for_regex)

    parsed_data = {"valid": False, "file_name": current_file_name_str, "extension": file_extension, "reason": "Padrão de nome não reconhecido", "name_part_before_revision": ""}

    if match_std1:
        year, cp, building, phase, discipline, seq, subdiscipline, rev = match_std1.groups()
        name_part_bf_rev = f"{str(year)}.{str(cp)}.{str(building)}.{str(phase)}.{str(discipline)}.{str(seq)}.{str(subdiscipline)}"
        parsed_data.update({"valid": True, "standard": 1, "year": str(year), "cp": str(cp), "building": str(building), "phase": str(phase),"discipline": str(discipline), "sequential": str(seq), "subdiscipline": str(subdiscipline), "revision": str(rev),"sheets_or_variant": None, "name_part_before_revision": name_part_bf_rev, "reason": "Padrão 01"})
    elif match_std2:
        year, cp, building, phase, discipline, seq, subdiscipline, sheets_variant, rev = match_std2.groups()
        name_part_bf_rev = f"{str(year)}.{str(cp)}.{str(building)}.{str(phase)}.{str(discipline)}.{str(seq)}.{str(subdiscipline)}.{str(sheets_variant)}"
        parsed_data.update({"valid": True, "standard": 2, "year": str(year), "cp": str(cp), "building": str(building), "phase": str(phase), "discipline": str(discipline), "sequential": str(seq), "subdiscipline": str(subdiscipline), "revision": str(rev), "sheets_or_variant": str(sheets_variant), "name_part_before_revision": name_part_bf_rev, "reason": "Padrão 02 (Isométrico)"})
    elif match_std3:
        year, cp, building, phase, discipline, seq, subdiscipline = match_std3.groups()
        name_part_bf_rev = name_part_for_regex
        parsed_data.update({
            "valid": True, "standard": 3, "year": str(year), "cp": str(cp), "building": str(building), "phase": str(phase),
            "discipline": str(discipline), "sequential": str(seq), "subdiscipline": str(subdiscipline),
            "revision": "", 
            "sheets_or_variant": None,
            "name_part_before_revision": name_part_bf_rev,
            "reason": "Padrão 03 (Sem Revisão)"
        })

    if parsed_data["valid"]:
        disc = str(parsed_data["discipline"])
        if disc not in DISCIPLINES_AND_SUBDISCIPLINES:
            parsed_data["valid"] = False
            parsed_data["reason"] += f" - Disciplina '{disc}' desconhecida."

        current_year_folder_str = str(year_folder)
        expected_year_short_from_folder = ""
        if current_year_folder_str and len(current_year_folder_str) >= 2:
            expected_year_short_from_folder = current_year_folder_str[-2:]

        year_matches = (str(parsed_data["year"]) == expected_year_short_from_folder) if current_year_folder_str else True
        if not year_matches:
            parsed_data["valid"] = False
            year_display_folder = expected_year_short_from_folder if expected_year_short_from_folder else "N/A"
            parsed_data["reason"] += f" - Ano ({parsed_data['year']}) != Ano pasta ({year_display_folder})."

        current_cp_folder_numeric_str = str(cp_folder_numeric)
        cp_matches = (str(parsed_data["cp"]) == current_cp_folder_numeric_str) if current_cp_folder_numeric_str else True
        if not cp_matches:
            parsed_data["valid"] = False
            cp_display_folder = current_cp_folder_numeric_str if current_cp_folder_numeric_str else "N/A"
            parsed_data["reason"] += f" - CP ({parsed_data['cp']}) != CP pasta ({cp_display_folder})."
    return parsed_data

def format_revision_suffix_for_comparison(revisao_g_str):
    revisao_g_str = str(revisao_g_str).strip()
    if not revisao_g_str: return ""
    revisao_upper = revisao_g_str.upper()
    if revisao_upper.startswith(".R") and len(revisao_upper) > 2:
        rest = revisao_upper[2:]
        if re.match(r"^\d+[A-Z]*$", rest): return revisao_upper
        if rest.isalnum(): return revisao_upper
    if revisao_upper.startswith("R") and len(revisao_upper) > 1:
        rest = revisao_upper[1:]
        if re.match(r"^\d+[A-Z]*$", rest):
            match_rev_num_alpha = re.match(r"(\d+)([A-Z]*)", rest)
            if match_rev_num_alpha:
                num_part = match_rev_num_alpha.group(1)
                alpha_part = match_rev_num_alpha.group(2)
                return f".R{num_part.zfill(2)}{alpha_part}"
        if rest.isalnum(): return f".{revisao_upper}"
    if revisao_upper.isdigit():
        return f".R{revisao_upper.zfill(2)}"
    if revisao_upper.isalnum(): 
        if re.match(r"^R\d{2}[A-Z]*$", revisao_upper):
            return f".{revisao_upper}"
        return f".{revisao_upper}" 
    return f".{revisao_upper}" 


def get_revision_value_for_sorting(revision_str):
    if not revision_str or not isinstance(revision_str, str): return (0, -2, "", revision_str if revision_str else "")
    rev_clean = revision_str.upper().lstrip('.')
    match_r_digit_alpha = re.match(r"R(\d+)([A-Z]*)", rev_clean)
    if match_r_digit_alpha:
        num_part = int(match_r_digit_alpha.group(1))
        alpha_part = match_r_digit_alpha.group(2)
        return (3, num_part, alpha_part, revision_str)
    match_digit_alpha = re.match(r"(\d+)([A-Z]+)", rev_clean)
    if match_digit_alpha:
        num_part = int(match_digit_alpha.group(1))
        alpha_part = match_digit_alpha.group(2)
        return (2, num_part, alpha_part, revision_str)
    match_digit_only = re.match(r"(\d+)$", rev_clean)
    if match_digit_only:
        num_part = int(match_digit_only.group(1))
        return (1, num_part, "", revision_str)
    if rev_clean.isalnum():
        return (0, 0, rev_clean, revision_str)
    return (-1, 0, rev_clean, revision_str)

def read_ld_google_sheet_with_creds(gspread_client_instance, spreadsheet_id, spreadsheet_name_for_debug=""):
    if not GSPREAD_AVAILABLE:
        log_message("ERRO: Biblioteca gspread não está disponível. Não é possível ler Google Sheet.")
        return []

    document_list = []
    try:
        log_message(f"INFO: Abrindo Google Sheet '{spreadsheet_name_for_debug}' (ID: {spreadsheet_id})...")
        spreadsheet = gspread_client_instance.open_by_key(spreadsheet_id)

        NOME_DA_ABA_LD = "PRJ-001-F1 LD"
        worksheet = None
        try:
            worksheet = spreadsheet.worksheet(NOME_DA_ABA_LD)
            log_message(f"INFO: Usando a planilha com nome específico '{NOME_DA_ABA_LD}': '{worksheet.title}'.")
        except gspread.exceptions.WorksheetNotFound:
            log_message(f"AVISO: A planilha com o nome '{NOME_DA_ABA_LD}' não foi encontrada em '{spreadsheet_name_for_debug}'. Tentando primeira aba...")
            try:
                worksheet = spreadsheet.get_worksheet(0)
                log_message(f"AVISO: Usando primeira planilha (índice 0) como fallback: '{worksheet.title}'.")
            except Exception as e_get_ws_fallback:
                log_message(f"ERRO FATAL: Falha ao obter a primeira planilha como fallback para '{spreadsheet_name_for_debug}': {e_get_ws_fallback}.")
                traceback.print_exc()
                return []
        except Exception as e_get_ws_named:
            log_message(f"ERRO FATAL ao tentar obter a planilha por nome '{NOME_DA_ABA_LD}' para '{spreadsheet_name_for_debug}': {e_get_ws_named}.")
            traceback.print_exc()
            return []

        if not worksheet:
            log_message(f"ERRO FATAL: Nenhuma planilha pôde ser carregada para '{spreadsheet_name_for_debug}'.")
            return []

        log_message(f"INFO: Lendo dados da planilha '{worksheet.title}' (total de {worksheet.row_count} linhas).")

        if worksheet.row_count < LD_SHEET_START_DATA_ROW :
             log_message(f"AVISO: Planilha LD '{spreadsheet_name_for_debug}' tem apenas {worksheet.row_count} linhas. "
                         f"Esperado pelo menos {LD_SHEET_START_DATA_ROW} para dados (considerando cabeçalhos/linhas puladas).")
             return []
        try:
            all_sheet_values = worksheet.get_all_values()
        except Exception as e_get_all:
            log_message(f"ERRO ao buscar todos os valores da planilha '{worksheet.title}': {e_get_all}")
            return []

        if not all_sheet_values:
            log_message(f"AVISO: Planilha '{worksheet.title}' de '{spreadsheet_name_for_debug}' está vazia ou não retornou dados.")
            return []

        header_row_index = LD_SHEET_START_DATA_ROW - 2
        first_data_row_index = LD_SHEET_START_DATA_ROW - 1 

        if header_row_index < 0 or header_row_index >= len(all_sheet_values):
            log_message(f"AVISO: Linha de cabeçalho calculada ({header_row_index+1}) fora dos limites da planilha '{worksheet.title}' ({len(all_sheet_values)} linhas). Tentando usar primeira linha como cabeçalho.")
            header_row_index = 0
            first_data_row_index = 1
            if len(all_sheet_values) <= 1:
                 log_message(f"AVISO: Planilha '{worksheet.title}' não tem linhas suficientes para cabeçalho e dados.")
                 return []

        headers = [str(h).strip().upper() for h in all_sheet_values[header_row_index]]
        data_rows = all_sheet_values[first_data_row_index:]

        if not data_rows:
            log_message(f"AVISO: Nenhuma linha de dados encontrada após cabeçalhos na planilha '{worksheet.title}'.")
            return []

        col_map_config_gsheet = {
            'full_name': ["NOME DO ARQUIVO", "FULL_NAME", "FILENAME", "DOCUMENTO", "CÓDIGO CLIENTE", "NOME DO DOCUMENTO"],
            'disciplina_ld': ["DISCIPLINA", "DISCIPLINA_LD", "DISC", "DISC."],
            'revisao_atual_col_g': ["REV ATUAL", "REVISÃO ATUAL", "REVISAO_ATUAL_COL_G", "REV G", "REVISÃO", "REV."],
            'situacao_ld': ["SITUAÇÃO", "SITUACAO_LD", "SITUACAO H", "STATUS H"],
            'subdisciplina_ld': ["SUBDISCIPLINA", "SUBDISCIPLINA_LD", "EDIFICAÇÃO", "SUB J", "SUBDISCIPLINAS", "SUB-DISCIPLINA"]
        }

        col_indices = {} 
        for key, possible_headers in col_map_config_gsheet.items():
            found_idx = -1
            for p_header in possible_headers:
                try:
                    found_idx = headers.index(p_header.upper())
                    break
                except ValueError:
                    continue
            if found_idx != -1:
                col_indices[key] = found_idx
            else:
                if key in ['full_name', 'revisao_atual_col_g']:
                    log_message(f"ERRO FATAL: Coluna essencial '{key}' (opções: {possible_headers}) não encontrada nos cabeçalhos da GSheet: {headers}")
                    return []
                elif key == 'disciplina_ld': 
                    log_message(f"AVISO: Coluna de disciplina '{key}' (opções: {possible_headers}) não encontrada. Será tentada derivação do nome do arquivo.")
                col_indices[key] = None

        processed_count = 0
        status_column_fixed_index = 14 

        for row_num_in_data_rows, row_values in enumerate(data_rows):
            filename_str = ""
            if col_indices.get('full_name') is not None and col_indices['full_name'] < len(row_values):
                filename_str = str(row_values[col_indices['full_name']]).strip().upper()

            if filename_str:
                doc_data = {'full_name': filename_str}
                for key, idx_mapped_col in col_indices.items():
                    if key == 'full_name': 
                        continue
                    if key == 'status_col_o': 
                        continue

                    if idx_mapped_col is not None and idx_mapped_col < len(row_values):
                        doc_data[key] = str(row_values[idx_mapped_col]).strip().upper()
                    else:
                        doc_data[key] = "" 
                if status_column_fixed_index < len(row_values):
                    doc_data['status_col_o'] = str(row_values[status_column_fixed_index]).strip().upper()
                else:
                    doc_data['status_col_o'] = ""
                    original_sheet_row_num = first_data_row_index + row_num_in_data_rows + 1 
                    log_message(f"AVISO (GSheet): Linha {original_sheet_row_num} da planilha não possui coluna O (índice {status_column_fixed_index}) para Status.")

                if not doc_data.get('disciplina_ld'):
                    parsed_fn_for_disc = parse_file_name(filename_str, "", "")
                    if parsed_fn_for_disc["valid"] and parsed_fn_for_disc.get("discipline"):
                        doc_data['disciplina_ld'] = parsed_fn_for_disc["discipline"]
                    else:
                        sigla_from_fn = extract_siglas_from_filename(filename_str)
                        doc_data['disciplina_ld'] = sigla_from_fn if sigla_from_fn else "INDEFINIDA"

                expected_keys_in_doc = ['full_name', 'disciplina_ld', 'revisao_atual_col_g', 'status_col_o', 'situacao_ld', 'subdisciplina_ld']
                for k_expected in expected_keys_in_doc:
                    if k_expected not in doc_data:
                        doc_data[k_expected] = "INDEFINIDA" if k_expected == 'disciplina_ld' else ""

                document_list.append(doc_data)
                processed_count += 1

        log_message(f"INFO: {processed_count} itens processados da LD Google Sheet '{spreadsheet_name_for_debug}'.")
        return document_list

    except gspread.exceptions.APIError as e_api:
        if hasattr(e_api, 'response') and e_api.response.status_code == 403:
             log_message(f"ERRO API GSpread (403) ao ler '{spreadsheet_name_for_debug}': {e_api}. ")
        elif hasattr(e_api, 'response') and e_api.response.status_code == 404:
             log_message(f"ERRO API GSpread (404) Planilha '{spreadsheet_name_for_debug}' (ID: {spreadsheet_id}) não encontrada.")
        else:
             log_message(f"ERRO API GSpread ao ler '{spreadsheet_name_for_debug}': {type(e_api).__name__} - {e_api}")
        traceback.print_exc()
        return []
    except Exception as e:
        log_message(f"Erro inesperado ao ler GSheet '{spreadsheet_name_for_debug}': {type(e).__name__} - {e}")
        traceback.print_exc()
        return []

def read_ld_csv_file(ld_csv_filepath):
    document_list = []
    delimiters_to_try = [';', ',']
    col_map_config = {
        'full_name': ["NOME DO ARQUIVO", "FULL_NAME", "FILENAME", "DOCUMENTO", "CÓDIGO CLIENTE", "NOME DO DOCUMENTO"],
        'disciplina_ld': ["DISCIPLINA", "DISCIPLINA_LD", "DISC", "DISC."],
        'revisao_atual_col_g': ["REVISÃO ATUAL", "REVISAO_ATUAL_COL_G", "REV G", "REVISÃO", "REV.", "REV ATUAL", "REV. ATUAL"],
        'situacao_ld': ["SITUAÇÃO", "SITUACAO_LD", "SITUACAO H", "STATUS H"],
        'subdisciplina_ld': ["SUBDISCIPLINA", "SUBDISCIPLINA_LD", "EDIFICAÇÃO", "SUB J", "SUBDISCIPLINAS", "SUB-DISCIPLINA"]
    }
    df = None; detected_delimiter = None
    log_message(f"INFO: Tentando ler arquivo CSV LD: '{ld_csv_filepath}'")

    rows_to_skip_val = list(range(LD_SHEET_START_DATA_ROW - 1)) if LD_SHEET_START_DATA_ROW > 1 else None
    log_message(f"DEBUG (read_ld_csv_file): Tentando pular {len(rows_to_skip_val) if rows_to_skip_val else 0} linhas. A próxima linha (linha {LD_SHEET_START_DATA_ROW-1}) será lida como cabeçalho.")

    for delim in delimiters_to_try:
        try:
            try:
                df_temp = pd.read_csv(ld_csv_filepath, sep=delim, encoding='utf-8-sig', skiprows=rows_to_skip_val, keep_default_na=False, dtype=str, low_memory=False)
            except UnicodeDecodeError:
                log_message(f"DEBUG: Falha CSV '{ld_csv_filepath}' com utf-8-sig, delim '{delim}'. Tentando latin-1.")
                df_temp = pd.read_csv(ld_csv_filepath, sep=delim, encoding='latin-1', skiprows=rows_to_skip_val, keep_default_na=False, dtype=str, low_memory=False)

            if df_temp.empty and rows_to_skip_val:
                log_message(f"AVISO: CSV '{ld_csv_filepath}' ficou vazio após pular {len(rows_to_skip_val)} linhas com delimitador '{delim}'.")
                continue

            temp_col_names_normalized_map = {str(col_name).strip().upper(): str(col_name) for col_name in df_temp.columns}
            log_message(f"DEBUG: Colunas lidas do CSV com delim '{delim}': {list(temp_col_names_normalized_map.keys())}")

            found_essential_cols = True
            for essential_key in ['full_name', 'revisao_atual_col_g']:
                if not any(candidate.upper() in temp_col_names_normalized_map for candidate in col_map_config.get(essential_key, [])):
                    found_essential_cols = False
                    log_message(f"DEBUG: Delim '{delim}' para '{ld_csv_filepath}' NÃO encontrou coluna essencial para '{essential_key}' (Opções: {col_map_config.get(essential_key, [])}). Colunas encontradas: {list(temp_col_names_normalized_map.keys())}")
                    break
            if found_essential_cols:
                df = df_temp; detected_delimiter = delim
                log_message(f"INFO: CSV '{ld_csv_filepath}' lido OK com delimitador '{detected_delimiter}'. Cabeçalhos normalizados: {list(temp_col_names_normalized_map.keys())}")
                break
            else: log_message(f"DEBUG: Delimitador '{delim}' não produziu colunas essenciais para '{ld_csv_filepath}'.")
        except pd.errors.EmptyDataError: log_message(f"AVISO: CSV '{ld_csv_filepath}' vazio ou após pular linhas (delim '{delim}')."); return []
        except Exception as e_read_csv: log_message(f"DEBUG: Falha ao ler CSV '{ld_csv_filepath}' com delimitador '{delim}': {type(e_read_csv).__name__} - {e_read_csv}")

    if df is None or df.empty:
        log_message(f"ERRO: Não foi possível ler CSV '{ld_csv_filepath}' ou colunas essenciais não encontradas, ou CSV ficou vazio após pular linhas.")
        return []

    final_col_names_map = {} 
    df_col_names_normalized_map = {str(col_name).strip().upper(): str(col_name) for col_name in df.columns}

    for standard_key, candidate_names_list in col_map_config.items():
        if standard_key == 'status_col_o':
            continue
        found_actual_name = next((df_col_names_normalized_map[candidate.upper()]
                                  for candidate in candidate_names_list
                                  if candidate.upper() in df_col_names_normalized_map), None)
        if found_actual_name:
            final_col_names_map[standard_key] = found_actual_name
            log_message(f"DEBUG: Mapeada chave '{standard_key}' para coluna CSV '{found_actual_name}'")
        else:
            if standard_key in ['full_name', 'revisao_atual_col_g']:
                log_message(f"ERRO FATAL: Coluna essencial '{standard_key}' (Opções: {candidate_names_list}) NÃO encontrada nos cabeçalhos do CSV: {list(df_col_names_normalized_map.keys())}")
                return []
            elif standard_key == 'disciplina_ld':
                 log_message(f"AVISO: Coluna de disciplina '{standard_key}' (Opções: {candidate_names_list}) não encontrada. Será tentada derivação do nome do arquivo.")
            final_col_names_map[standard_key] = None

    status_column_csv_physical_index = 14 

    for index_row_df, row in df.iterrows():
        filename_val = ""
        fn_col_name_actual = final_col_names_map.get('full_name')
        if fn_col_name_actual and fn_col_name_actual in row.index: 
            filename_val = str(row.get(fn_col_name_actual, "")).strip().upper()
        elif not fn_col_name_actual and 0 < len(df.columns): 
             log_message(f"AVISO (CSV): Tentando primeira coluna '{df.columns[0]}' como nome de arquivo para linha {index_row_df}, pois 'full_name' não foi mapeado.")
             filename_val = str(row.iloc[0]).strip().upper() 

        if filename_val:
            disciplina_val = "INDEFINIDA"
            disciplina_col_name_actual = final_col_names_map.get('disciplina_ld')
            if disciplina_col_name_actual and disciplina_col_name_actual in row.index:
                disciplina_val = str(row.get(disciplina_col_name_actual, "INDEFINIDA")).strip().upper() or "INDEFINIDA"
            if disciplina_val == "INDEFINIDA": 
                parsed_fn_for_disc_csv = parse_file_name(filename_val, "", "")
                if parsed_fn_for_disc_csv["valid"] and parsed_fn_for_disc_csv.get("discipline"):
                    disciplina_val = parsed_fn_for_disc_csv["discipline"]
                else:
                    sigla_from_fn_csv = extract_siglas_from_filename(filename_val)
                    disciplina_val = sigla_from_fn_csv if sigla_from_fn_csv else "INDEFINIDA"

            revisao_val = ""
            revisao_col_name_actual = final_col_names_map.get('revisao_atual_col_g')
            if revisao_col_name_actual and revisao_col_name_actual in row.index:
                revisao_val = str(row.get(revisao_col_name_actual, "")).strip().upper()

            status_val = ""
            if status_column_csv_physical_index < len(df.columns):
                status_actual_col_name_in_df = df.columns[status_column_csv_physical_index]
                if status_actual_col_name_in_df in row.index:
                    status_val = str(row.get(status_actual_col_name_in_df, "")).strip().upper()
                else: 
                    log_message(f"AVISO (CSV): Nome da coluna de status '{status_actual_col_name_in_df}' não encontrado na linha {index_row_df} do DataFrame.")
                    status_val = ""
            else:
                log_message(f"AVISO (CSV): DataFrame não possui {status_column_csv_physical_index + 1} colunas para ler Status pela posição 'O'. Linha do DataFrame: {index_row_df} (planilha original linha aprox. {LD_SHEET_START_DATA_ROW + index_row_df}).")
                status_val = ""

            situacao_val = ""
            situacao_col_name_actual = final_col_names_map.get('situacao_ld')
            if situacao_col_name_actual and situacao_col_name_actual in row.index:
                situacao_val = str(row.get(situacao_col_name_actual, "")).strip().upper()

            subdisciplina_val = ""
            subdisciplina_col_name_actual = final_col_names_map.get('subdisciplina_ld')
            if subdisciplina_col_name_actual and subdisciplina_col_name_actual in row.index:
                subdisciplina_val = str(row.get(subdisciplina_col_name_actual, "")).strip().upper()

            document_list.append({
                'full_name': filename_val,
                'disciplina_ld': disciplina_val,
                'revisao_atual_col_g': revisao_val,
                'status_col_o': status_val, 
                'situacao_ld': situacao_val,
                'subdisciplina_ld': subdisciplina_val
            })
            if len(document_list) < 5:
                 log_message(f"DEBUG (read_ld_csv_file) - Item LD processado: {document_list[-1]}")

    if not document_list and not df.empty: log_message(f"AVISO: Nenhum documento com nome de arquivo válido extraído de '{ld_csv_filepath}', apesar do DataFrame ter {len(df)} linhas.")
    log_message(f"INFO: {len(document_list)} itens processados do CSV LD '{ld_csv_filepath}'.")
    return document_list


def normalize_drive_filename_for_comparison(filename_str):
    if not filename_str: return ""
    name_part, _ = os.path.splitext(str(filename_str)); return name_part.lower()

def normalize_ld_item_name_with_revision(ld_item_info):
    full_name_from_ld = ld_item_info.get('full_name', "").strip()
    revisao_g_str = ld_item_info.get('revisao_atual_col_g', "").strip()
    if not full_name_from_ld: return ""

    revision_suffix_formatted = format_revision_suffix_for_comparison(revisao_g_str)
    parsed_ld_fn = parse_file_name(full_name_from_ld, "", "")

    if parsed_ld_fn["valid"] and "name_part_before_revision" in parsed_ld_fn and parsed_ld_fn["name_part_before_revision"]:
        base_name_ld_std = parsed_ld_fn["name_part_before_revision"]
        return f"{base_name_ld_std.lower()}{revision_suffix_formatted.lower()}"
    else:
        base_name_ld_no_ext, _ = os.path.splitext(full_name_from_ld)
        return f"{base_name_ld_no_ext.lower()}{revision_suffix_formatted.lower()}"


def get_project_discipline_folder_map(drive_service_instance, project_folder_drive_id):
    mapping = {}
    if not project_folder_drive_id: log_message("AVISO (get_project_discipline_folder_map): ID da pasta do projeto não fornecido."); return mapping
    try:
        res = drive_service_instance.files().list(q=f"'{project_folder_drive_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false", fields="files(name, id)", pageSize=PAGE_SIZE, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        for folder in res.get('files', []):
            folder_name_original = folder.get('name', ''); folder_name_upper = folder_name_original.upper()
            if folder_name_upper in FOLDER_NAME_TO_DISCIPLINE_SIGLA_MAP: mapping[folder_name_original] = FOLDER_NAME_TO_DISCIPLINE_SIGLA_MAP[folder_name_upper]
    except HttpError as e: log_message(f"ERRO API ao buscar pastas de disciplina no Drive para ID '{project_folder_drive_id}': {e}")
    except Exception as e_gen: log_message(f"ERRO INESPERADO em get_project_discipline_folder_map para ID '{project_folder_drive_id}': {e_gen}")
    return mapping

def _get_progress_bar_callable():
    global _alive_bar_imported_successfully
    if _alive_bar_imported_successfully: from alive_progress import alive_bar as progress_bar_class; return progress_bar_class # type: ignore
    else: return alive_bar

@cached(DRIVE_CACHE, key=lambda drive_service_instance, folder_id, project_discipline_folders_map, project_name_desc, gui_app_ref: (folder_id, project_name_desc))
def list_drive_files_recursive(drive_service_instance, folder_id, project_discipline_folders_map=None, project_name_desc="", gui_app_ref=None):
    if project_discipline_folders_map is None: project_discipline_folders_map = {}
    files_by_discipline_drive = {"NAO_MAPEADO": {}, "INVALIDOS": {}}
    items_to_process = [{'id': folder_id, 'path_names': []}]; processed_folders = set()

    year_folder_from_project_name, cp_folder_from_project_name_numeric = "", ""
    if project_name_desc:
        name_parts = project_name_desc.split(" - ", 1); code_part = name_parts[0]
        year_cp_match = re.search(r"(?:[LM])?(\d{2,4})[_.](\d+)", code_part)
        if year_cp_match:
            year_digits, cp_digits = year_cp_match.group(1), year_cp_match.group(2)
            if len(year_digits) == 4: year_folder_from_project_name = year_digits
            elif len(year_digits) == 2: year_folder_from_project_name = "20" + year_digits
            cp_folder_from_project_name_numeric = cp_digits
        else:
            simple_match = re.match(r"([A-Z]?)(\d+)[_.](\d+)", code_part)
            if simple_match:
                year_digits_simple, cp_digits_simple = simple_match.group(2), simple_match.group(3)
                if len(year_digits_simple) == 2: year_folder_from_project_name = "20" + year_digits_simple
                elif len(year_digits_simple) == 4: year_folder_from_project_name = year_digits_simple
                cp_folder_from_project_name_numeric = cp_digits_simple
        if year_folder_from_project_name or cp_folder_from_project_name_numeric:
            log_message(f"DEBUG (DriveScan): Projeto '{project_name_desc}', ano para parse='{year_folder_from_project_name}', CP para parse='{cp_folder_from_project_name_numeric}'.")

    folder_count = 0; file_scan_count = 0
    Progress_Bar_Class = _get_progress_bar_callable()

    with Progress_Bar_Class(total=None, title=f"Listando Drive ({project_name_desc[:25]})", unit="item API", disable=(gui_app_ref is not None)) as pbar_items:
        while items_to_process:
            current_folder_item = items_to_process.pop(0)
            current_folder_id, current_path_names = current_folder_item['id'], current_folder_item['path_names']
            folder_display_name = '/'.join(current_path_names) if current_path_names else project_name_desc

            if current_folder_id in processed_folders: continue
            processed_folders.add(current_folder_id); folder_count += 1
            pbar_items.text(f"Listando: {folder_display_name[:30]}...")
            if gui_app_ref and folder_count % 5 == 0 and hasattr(gui_app_ref, 'update_progress_display') :
                gui_app_ref.master.after(0, lambda: gui_app_ref.update_progress_display(text=f"Listando Drive: {folder_display_name[:30]}... ({folder_count} pastas)"))

            page_token = None
            try:
                while True:
                    response = drive_service_instance.files().list(
                        q=f"'{current_folder_id}' in parents and trashed=false",
                        fields="nextPageToken, files(id, name, mimeType)",
                        pageSize=PAGE_SIZE,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        pageToken=page_token
                    ).execute()

                    for file_item in response.get('files', []):
                        file_scan_count+=1
                        pbar_items()

                        original_file_name = str(file_item.get('name', ''))
                        file_id, file_mime = file_item.get('id'), file_item.get('mimeType')
                        if not original_file_name or not file_id or not file_mime:
                            log_message(f"AVISO (DriveScan): Item Drive com campos ausentes: {file_item}. Pulando."); continue

                        if file_mime == 'application/vnd.google-apps.folder':
                            items_to_process.append({'id': file_id, 'path_names': current_path_names + [original_file_name]}); continue

                        file_ext_lower = Path(original_file_name).suffix.lower()
                        if ALLOWED_EXTENSIONS_DRIVE_SEARCH and not any(file_ext_lower == f".{ext.lower()}" for ext in ALLOWED_EXTENSIONS_DRIVE_SEARCH):
                            continue

                        parsed_file_info = parse_file_name(original_file_name, year_folder_from_project_name, cp_folder_from_project_name_numeric)

                        discipline_key_for_grouping = "INVALIDOS"
                        if parsed_file_info["valid"] and parsed_file_info.get("discipline"):
                             discipline_key_for_grouping = parsed_file_info["discipline"]
                        else:
                            found_discipline_in_path = False
                            if current_path_names:
                                for folder_name_in_path in reversed(current_path_names):
                                    folder_name_upper = str(folder_name_in_path).upper()
                                    if folder_name_upper in FOLDER_NAME_TO_DISCIPLINE_SIGLA_MAP:
                                        discipline_key_for_grouping = FOLDER_NAME_TO_DISCIPLINE_SIGLA_MAP[folder_name_upper]
                                        found_discipline_in_path = True; break
                            if not found_discipline_in_path:
                                sigla_from_name = extract_siglas_from_filename(original_file_name)
                                if sigla_from_name: discipline_key_for_grouping = sigla_from_name
                                else: discipline_key_for_grouping = "NAO_MAPEADO"

                            if parsed_file_info["reason"] != "Padrão de nome não reconhecido":
                                log_message(f"AVISO (DriveScan): Arquivo '{original_file_name}' em '{folder_display_name}' inválido ({parsed_file_info.get('reason', 'desconhecido')}), classificado como '{discipline_key_for_grouping}'.")

                        drive_std_base_name_key = ""
                        if parsed_file_info["valid"] and parsed_file_info.get("name_part_before_revision"):
                            drive_std_base_name_key = parsed_file_info["name_part_before_revision"].lower()
                        else:
                            norm_base, _ = normalize_name_for_comparison(original_file_name)
                            drive_std_base_name_key = norm_base

                        if not drive_std_base_name_key:
                            log_message(f"AVISO (DriveScan): Chave base nula para arquivo '{original_file_name}'. Pulando.")
                            continue

                        if discipline_key_for_grouping not in files_by_discipline_drive:
                            files_by_discipline_drive[discipline_key_for_grouping] = {}
                        if drive_std_base_name_key not in files_by_discipline_drive[discipline_key_for_grouping]:
                            files_by_discipline_drive[discipline_key_for_grouping][drive_std_base_name_key] = []

                        files_by_discipline_drive[discipline_key_for_grouping][drive_std_base_name_key].append({
                            "name": original_file_name,
                            "id": file_id,
                            "mimeType": file_mime,
                            "parsed_info": parsed_file_info
                        })

                    page_token = response.get('nextPageToken')
                    if not page_token: break
            except HttpError as error_list:
                status_code = error_list.resp.status if hasattr(error_list, 'resp') else 'N/A'; reason = error_list._get_reason() if hasattr(error_list, '_get_reason') else 'Desconhecida'
                log_message(f"ERRO API listar pasta '{current_folder_id}' ({folder_display_name}): Status {status_code}, Razão: {reason}.")
            except Exception as e_gen_list: log_message(f"ERRO INESPERADO listar Drive pasta '{current_folder_id}' ({folder_display_name}): {type(e_gen_list).__name__} - {e_gen_list}")

    if gui_app_ref and hasattr(gui_app_ref, 'update_progress_display'):
        gui_app_ref.master.after(0, lambda: gui_app_ref.update_progress_display(text=f"Listagem Drive {project_name_desc[:25]} concluída. {file_scan_count} arquivos.", mode='determinate', value=100))
    return files_by_discipline_drive


def generate_divergence_report_excel(project_name_local, report_data_by_discipline, output_dir="."):
    safe_name = "".join(c if c.isalnum() else "_" for c in project_name_local)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    fn_path = output_path / f"Relatorio_Divergencias_Consolidado_{safe_name}_{ts}.xlsx"
    fn = str(fn_path)
    excel_engine = 'xlsxwriter' if XLSXWRITER_AVAILABLE else 'openpyxl'
    log_message(f"INFO: Usando engine '{excel_engine}' para gerar Excel de divergências para '{project_name_local}'. Destino: {fn}")

    try:
        with pd.ExcelWriter(fn, engine=excel_engine) as writer:
            df_meta = pd.DataFrame([
                ("Relatório", "Divergências LD x Drive (Por Sigla de Disciplina)"), 
                ("Gerado em", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("Projeto Analisado", project_name_local),
            ], columns=["Parâmetro", "Valor"])
            df_meta.to_excel(writer, sheet_name="Metadados", index=False)
            if excel_engine == 'xlsxwriter':
                workbook = writer.book
                worksheet_meta = writer.sheets["Metadados"]
                header_format_meta = workbook.add_format({'bold': True, 'bg_color': '#F0F0F0', 'border':1})
                for col_num, value in enumerate(df_meta.columns.values):
                    worksheet_meta.write(0, col_num, value, header_format_meta)
                    worksheet_meta.set_column(col_num, col_num, len(str(value)) + 5)

            if not report_data_by_discipline:
                df_empty = pd.DataFrame({"Status": ["NENHUMA DIVERGÊNCIA OU DADO PARA RELATAR."]})
                df_empty.to_excel(writer, sheet_name="Resumo_Sem_Dados", index=False)
                if excel_engine == 'xlsxwriter' and "Resumo_Sem_Dados" in writer.sheets:
                    worksheet = writer.sheets["Resumo_Sem_Dados"]
                    worksheet.set_column(0, 0, len(df_empty.columns[0]) + 10)
                log_message(f"Relatório de divergências (vazio) gerado: {fn}")
                return fn

            log_message("INFO: Agrupando dados por sigla de disciplina para as abas do Excel...")
            data_for_sheets_by_sigla = {}
            for key_from_report, data_dict_items in report_data_by_discipline.items():
                original_key_upper = str(key_from_report).upper()
                canonical_sigla = DISCIPLINE_FULLNAME_TO_SIGLA_MAP.get(original_key_upper, original_key_upper)

                if canonical_sigla not in data_for_sheets_by_sigla:
                    data_for_sheets_by_sigla[canonical_sigla] = {
                        "only_ld_items_full_info": list(data_dict_items.get("only_ld_items_full_info", [])),
                        "only_drive_original_filenames": sorted(list(set(data_dict_items.get("only_drive_original_filenames", []))))
                    }
                else:
                    data_for_sheets_by_sigla[canonical_sigla]["only_ld_items_full_info"].extend(data_dict_items.get("only_ld_items_full_info", []))
                    
                    current_drive_files = set(data_for_sheets_by_sigla[canonical_sigla].get("only_drive_original_filenames", []))
                    current_drive_files.update(data_dict_items.get("only_drive_original_filenames", []))
                    data_for_sheets_by_sigla[canonical_sigla]["only_drive_original_filenames"] = sorted(list(current_drive_files))
            
            for sheet_name_key in sorted(data_for_sheets_by_sigla.keys()): 
                data = data_for_sheets_by_sigla[sheet_name_key]          
                only_ld_items = data.get("only_ld_items_full_info", [])
                only_drive_files = data.get("only_drive_original_filenames", [])
                
                if not only_ld_items and not only_drive_files:
                    log_message(f"INFO: Sigla de disciplina '{sheet_name_key}' sem dados para relatório. Pulando esta aba.")
                    continue

                df_rows = []
                processed_ld_keys = set()

                for item_ld_original_dict in only_ld_items:
                    ld_doc_name = str(item_ld_original_dict.get('full_name', '')).upper()
                    ld_rev = str(item_ld_original_dict.get('revisao_atual_col_g', '')).upper()
                    unique_key = f"{ld_doc_name}|{ld_rev}"

                    if not ld_doc_name or unique_key in processed_ld_keys:
                        continue
                    processed_ld_keys.add(unique_key)

                    comment = item_ld_original_dict.get('_comment', '')
                    situacao_verif = "APENAS NA LD"
                    if comment:
                        situacao_verif += f" ({comment})"

                    df_rows.append({
                        new_col_A_name: str(item_ld_original_dict.get('full_name', '')).upper(),
                        "REVISAO_LD (V01)": str(item_ld_original_dict.get('revisao_atual_col_g', '')).upper(),
                        "STATUS_LD (V01)": str(item_ld_original_dict.get('status_col_o', '')).upper(),
                        "SITUACAO_VERIFICACAO (V01)": situacao_verif,
                    })

                for drive_file_name_from_list in only_drive_files:
                    actual_file_name_for_col_A = str(drive_file_name_from_list)
                    situacao_verif_drive = "APENAS NO DRIVE"

                    marker_divergence = " *Relacionado a item LD com revisão divergente: "
                    if marker_divergence in actual_file_name_for_col_A:
                        situacao_verif_drive = "NO DRIVE (Revisão diverge de item LD similar)"
                        actual_file_name_for_col_A = actual_file_name_for_col_A.split(marker_divergence)[0].strip()
                    elif "(Rev:" in actual_file_name_for_col_A :
                         pass

                    df_rows.append({
                        new_col_A_name: actual_file_name_for_col_A.upper(),
                        "REVISAO_LD (V01)": "",
                        "STATUS_LD (V01)": "",
                        "SITUACAO_VERIFICACAO (V01)": situacao_verif_drive,
                    })

                if df_rows:
                    df_discipline = pd.DataFrame(df_rows)
                    ordered_cols = [new_col_A_name, "REVISAO_LD (V01)", "STATUS_LD (V01)", "SITUACAO_VERIFICACAO (V01)"]
                    df_discipline = df_discipline[[col for col in ordered_cols if col in df_discipline.columns]]

                    sheet_name_excel = re.sub(r'[\\/*?:\[\]]', '', str(sheet_name_key))[:31]
                    log_message(f"INFO: Escrevendo aba '{sheet_name_excel}' (originada de '{sheet_name_key}') no relatório.")
                    df_discipline.to_excel(writer, sheet_name=sheet_name_excel, index=False)

                    if excel_engine == 'xlsxwriter' and sheet_name_excel in writer.sheets:
                        workbook = writer.book 
                        worksheet = writer.sheets[sheet_name_excel]
                        header_format = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center'})
                        cell_format_wrap = workbook.add_format({'text_wrap': True, 'valign': 'top'})

                        for col_num, value in enumerate(df_discipline.columns.values):
                            worksheet.write(0, col_num, value, header_format)
                            column_data = df_discipline[value].astype(str)
                            max_len = column_data.map(len).max() if not column_data.empty else 0
                            header_len = len(str(value))

                            adjusted_width = 0
                            if value == new_col_A_name:
                                adjusted_width = max(40, min(max(max_len, header_len) + 5, 75))
                            elif value == "REVISAO_LD (V01)":
                                adjusted_width = max(15, min(max(max_len, header_len) + 3, 25))
                            elif value == "STATUS_LD (V01)":
                                adjusted_width = max(15, min(max(max_len, header_len) + 3, 30))
                            elif value == "SITUACAO_VERIFICACAO (V01)":
                                adjusted_width = max(35, min(max(max_len, header_len) + 5, 75))
                            else:
                                adjusted_width = max(15, min(max(max_len, header_len) + 3, 50))

                            worksheet.set_column(col_num, col_num, adjusted_width, cell_format_wrap)
                        worksheet.freeze_panes(1, 0)
                        if len(df_discipline) > 0:
                            worksheet.autofilter(0, 0, len(df_discipline), len(df_discipline.columns) - 1)

        log_message(f"Relatório de divergências (por sigla de disciplina) gerado com sucesso: {fn}")
        return fn
    except PermissionError as e_perm:
        log_message(f"ERRO DE PERMISSÃO ao gerar Excel '{fn}': {e_perm}.")
        if TKINTER_AVAILABLE and app_gui_instance: app_gui_instance.master.after(0, lambda: messagebox.showerror("Erro de Permissão (Excel)", f"Não foi possível salvar o relatório em:\n{fn}\n\nVerifique se o arquivo está aberto ou se há permissões de escrita.\n\nDetalhe: {e_perm}"))
        return None
    except Exception as e_excel:
        log_message(f"ERRO CRÍTICO ao gerar Excel de divergências '{fn}': {e_excel}")
        traceback.print_exc()
        if TKINTER_AVAILABLE and app_gui_instance: app_gui_instance.master.after(0, lambda: messagebox.showerror("Erro ao Gerar Excel", f"Ocorreu um erro inesperado ao gerar o relatório Excel:\n{e_excel}"))
        return None

def post_process_divergence_report(excel_file_path, output_dir="."):
    if not Path(excel_file_path).exists():
        log_message(f"ERRO (Pós-processamento): Arquivo de relatório não encontrado em '{excel_file_path}'")
        return None
    try:
        xls = pd.ExcelFile(excel_file_path)
        sheet_names = xls.sheet_names
    except Exception as e:
        log_message(f"ERRO (Pós-processamento): Não foi possível ler o arquivo Excel '{excel_file_path}'. Detalhe: {e}")
        return None

    original_file_path = Path(excel_file_path)
    new_file_name = f"{original_file_path.stem}_limpo{original_file_path.suffix}"
    output_path = Path(output_dir); output_path.mkdir(parents=True, exist_ok=True)
    cleaned_excel_path = output_path / new_file_name

    log_message(f"INFO (Pós-processamento): Iniciando limpeza do relatório '{excel_file_path}'.")
    log_message(f"INFO (Pós-processamento): Relatório limpo será salvo como '{cleaned_excel_path}'.")

    excel_writer_engine = 'openpyxl'
    if XLSXWRITER_AVAILABLE and cleaned_excel_path.suffix.lower() == '.xlsx':
        excel_writer_engine = 'xlsxwriter'

    try:
        with pd.ExcelWriter(cleaned_excel_path, engine=excel_writer_engine) as writer:
            for sheet_name in sheet_names:
                log_message(f"INFO (Pós-processamento): Processando planilha '{sheet_name}'...")
                df = xls.parse(sheet_name)

                expected_cols_for_cleaning = [new_col_A_name, "REVISAO_LD (V01)", "SITUACAO_VERIFICACAO (V01)"]

                if sheet_name == "Metadados" or df.empty or not all(col in df.columns for col in expected_cols_for_cleaning):
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    log_message(f"INFO (Pós-processamento): Planilha '{sheet_name}' copiada como está (sem limpeza de pares ou colunas não encontradas).")
                    continue

                df['original_index'] = df.index
                ld_items_to_match = []
                drive_items_to_match = []

                for index, row in df.iterrows():
                    situacao = str(row.get("SITUACAO_VERIFICACAO (V01)", "")).strip().upper()
                    arquivo_col_A_val = str(row.get(new_col_A_name, "")).strip().upper()
                    rev_ld_val = str(row.get("REVISAO_LD (V01)", "")).strip().upper()

                    base_name = None; norm_rev = None

                    if "APENAS NA LD" in situacao and arquivo_col_A_val:
                        parsed_name_ld = parse_file_name(arquivo_col_A_val, "", "")
                        if parsed_name_ld["valid"] and parsed_name_ld.get("name_part_before_revision"):
                            base_name = parsed_name_ld["name_part_before_revision"].upper()
                        else:
                            temp_norm_base, _ = normalize_name_for_comparison(arquivo_col_A_val)
                            base_name = temp_norm_base.upper()

                        rev_to_format_ld = rev_ld_val if rev_ld_val else (parsed_name_ld.get("revision", "") if parsed_name_ld["valid"] else "")
                        norm_rev = format_revision_suffix_for_comparison(rev_to_format_ld).upper()

                        if base_name and norm_rev is not None:
                            ld_items_to_match.append({"base_name": base_name, "norm_rev": norm_rev, "original_index": index})

                    elif ("APENAS NO DRIVE" in situacao or "NO DRIVE (REVISÃO DIVERGE" in situacao) and arquivo_col_A_val:
                        drive_file_name_for_parse = arquivo_col_A_val
                        clean_drive_name_for_parse = re.sub(r'\s*\(REV:.*?\)\s*$', '', drive_file_name_for_parse).strip()

                        parsed_name_drive = parse_file_name(clean_drive_name_for_parse, "", "")
                        if parsed_name_drive["valid"] and parsed_name_drive.get("name_part_before_revision"):
                            base_name = parsed_name_drive["name_part_before_revision"].upper()
                            norm_rev = format_revision_suffix_for_comparison(parsed_name_drive.get("revision", "")).upper()
                        else:
                            temp_norm_base_drive, _ = normalize_name_for_comparison(clean_drive_name_for_parse)
                            base_name = temp_norm_base_drive.upper()
                            extracted_rev_str_drive = extract_revision_from_filename(clean_drive_name_for_parse) 
                            norm_rev = format_revision_suffix_for_comparison(extracted_rev_str_drive.lstrip('.') if extracted_rev_str_drive else "").upper()

                        if base_name and norm_rev is not None:
                            drive_items_to_match.append({"base_name": base_name, "norm_rev": norm_rev, "original_index": index})

                indices_to_remove = set()
                available_drive_items = list(drive_items_to_match)
                for ld_item in ld_items_to_match:
                    for i, drive_item in enumerate(available_drive_items):
                        if ld_item["base_name"] == drive_item["base_name"] and \
                           ld_item["norm_rev"] == drive_item["norm_rev"]:
                            indices_to_remove.add(ld_item["original_index"])
                            indices_to_remove.add(drive_item["original_index"])
                            available_drive_items.pop(i)
                            break

                if indices_to_remove:
                    log_message(f"INFO (Pós-processamento): {len(indices_to_remove)} linhas ({len(indices_to_remove)//2} pares) marcadas para remoção na planilha '{sheet_name}'.")
                    df_cleaned = df[~df['original_index'].isin(indices_to_remove)].copy()
                    df_cleaned.drop(columns=['original_index'], inplace=True, errors='ignore')
                    df_cleaned.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    log_message(f"INFO (Pós-processamento): Nenhuma linha de par LD/Drive removida na planilha '{sheet_name}'.")
                    df_original_no_idx = df.drop(columns=['original_index'], errors='ignore')
                    df_original_no_idx.to_excel(writer, sheet_name=sheet_name, index=False)

        log_message(f"INFO (Pós-processamento): Relatório limpo salvo com sucesso em '{cleaned_excel_path}'.")
        return str(cleaned_excel_path)
    except Exception as e:
        log_message(f"ERRO CRÍTICO (Pós-processamento): Falha ao pós-processar o relatório '{excel_file_path}'. Detalhe: {e}")
        traceback.print_exc()
        if TKINTER_AVAILABLE and app_gui_instance and hasattr(app_gui_instance, 'master') and app_gui_instance.master.winfo_exists():
            app_gui_instance.master.after(0, lambda: messagebox.showerror("Erro Pós-Processamento", f"Falha ao limpar o relatório Excel:\n{e}"))
        return None

def clear_drive_cache(): global DRIVE_CACHE; DRIVE_CACHE.clear()

def estimate_total_files_in_drive_folder(drive_inst, root_folder_id, allowed_extensions):
    if not drive_inst or not root_folder_id: return 0
    total_files_estimate = 0; folders_to_scan_ids = [root_folder_id]; scanned_folder_ids = set()
    ext_query_part = ""
    if allowed_extensions:
        ext_conditions = [f"name contains '.{ext.lower()}'" for ext in allowed_extensions if ext]
        if ext_conditions: ext_query_part = f" and ({' or '.join(ext_conditions)})"
    max_folders_to_estimate = 100; folders_estimated_count = 0
    while folders_to_scan_ids and folders_estimated_count < max_folders_to_estimate:
        current_folder_id_to_scan = folders_to_scan_ids.pop(0)
        if current_folder_id_to_scan in scanned_folder_ids: continue
        scanned_folder_ids.add(current_folder_id_to_scan); folders_estimated_count +=1
        page_token_files = None
        try:
            while True:
                files_query = f"'{current_folder_id_to_scan}' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'{ext_query_part}"
                files_response = drive_inst.files().list(q=files_query, fields="nextPageToken, files(id)", pageSize=PAGE_SIZE, supportsAllDrives=True, includeItemsFromAllDrives=True, pageToken=page_token_files).execute()
                files_in_folder = files_response.get('files', [])
                total_files_estimate += len(files_in_folder)
                page_token_files = files_response.get('nextPageToken');
                if not page_token_files: break
            if folders_estimated_count < max_folders_to_estimate:
                page_token_folders = None
                while True:
                    subfolders_query = f"'{current_folder_id_to_scan}' in parents and trashed=false and mimeType = 'application/vnd.google-apps.folder'"
                    subfolders_response = drive_inst.files().list(q=subfolders_query, fields="nextPageToken, files(id)", pageSize=PAGE_SIZE, supportsAllDrives=True, includeItemsFromAllDrives=True, pageToken=page_token_folders).execute()
                    for subfolder in subfolders_response.get('files', []):
                        if subfolder['id'] not in scanned_folder_ids: folders_to_scan_ids.append(subfolder['id'])
                    page_token_folders = subfolders_response.get('nextPageToken')
                    if not page_token_folders: break
        except HttpError as e: log_message(f"AVISO (estimação): Erro ao estimar pasta '{current_folder_id_to_scan}': {e}.")
        except Exception as e_gen: log_message(f"AVISO (estimação): Erro geral ao estimar pasta '{current_folder_id_to_scan}': {e_gen}.")
    if folders_estimated_count >= max_folders_to_estimate and folders_to_scan_ids:
        log_message(f"AVISO: Estimativa de total de arquivos limitada a {max_folders_to_estimate} pastas.")
    return total_files_estimate

def normalize_name_for_comparison(filename):
    if not filename: return "", ""
    name_part, ext = os.path.splitext(str(filename))
    name_part = name_part.lower()
    name_part = re.sub(r'[._-]r\d{2}[a-z]*$', '', name_part, flags=re.IGNORECASE)
    name_part = re.sub(r'\.r\d{2}[a-z]*$', '', name_part, flags=re.IGNORECASE)
    return name_part.strip(), ext.lower()


def extract_revision_from_filename(filename_sem_ext): 
    if not filename_sem_ext: return None
    name_part = str(filename_sem_ext)
    match = re.search(r'([._-])?(R\d{2}[A-Z]*)$', name_part, re.IGNORECASE)
    if match:
        delimiter = match.group(1) or "." 
        revision_part = match.group(2).upper() 
        return f"{delimiter}{revision_part}" 

    match_simple_rxx = re.search(r'(R\d{2}[A-Z]*)$', name_part, re.IGNORECASE)
    if match_simple_rxx:
        return f".{match_simple_rxx.group(1).upper()}" 

    return None

def _helper_get_ld_base_rev_from_ld_item(ld_full_name_original, ld_revision_g_original):
    """
    Função auxiliar para obter a base normalizada e a revisão normalizada de um item da LD.
    Usa as lógicas de parse e normalização já definidas.
    """
    # Globais/importadas necessárias: parse_file_name, ALLOWED_EXTENSIONS_DRIVE_SEARCH, 
    # normalize_name_for_comparison, format_revision_suffix_for_comparison
    
    ld_base_norm = ""
    # Usa parse_file_name diretamente com o nome original da LD
    parsed_ld_fn_info = parse_file_name(ld_full_name_original, "", "") 

    if parsed_ld_fn_info.get("valid") and parsed_ld_fn_info.get("name_part_before_revision"):
        ld_base_norm = parsed_ld_fn_info["name_part_before_revision"].lower()
    else:
        _ , potential_ext_in_ld_name = os.path.splitext(ld_full_name_original)
        # ALLOWED_EXTENSIONS_DRIVE_SEARCH é global
        common_file_extensions_with_dot = [f".{ext.lower()}" for ext in ALLOWED_EXTENSIONS_DRIVE_SEARCH]

        if potential_ext_in_ld_name.lower() not in common_file_extensions_with_dot:
            ld_base_norm = ld_full_name_original.lower()
        else:
            temp_ld_base, _ = normalize_name_for_comparison(ld_full_name_original)
            ld_base_norm = temp_ld_base.lower()
            
    ld_rev_norm = format_revision_suffix_for_comparison(ld_revision_g_original).lower()
    return ld_base_norm, ld_rev_norm, parsed_ld_fn_info # parsed_ld_fn_info pode ser usado para debug

def perform_flexible_comparison(ld_documents, files_by_discipline_drive):
    global log_message, format_revision_suffix_for_comparison, normalize_name_for_comparison, extract_revision_from_filename, parse_file_name, ALLOWED_EXTENSIONS_DRIVE_SEARCH, get_revision_value_for_sorting

    TARGET_LD_FILENAME_DEBUG = "25.2016.00.PC.ARQ.004.PLA" 
    ENABLE_DETAILED_FILE_LOGGING = False 

    divergences = {"only_ld": [], "only_drive": [], "revision_mismatch": []}
    found_drive_original_names = set() 

    # 1. Construir mapa de revisões máximas da LD
    ld_max_revisions_map = {} 
    if ENABLE_DETAILED_FILE_LOGGING:
        log_message(f"DEBUG_FLEX_V3: Iniciando construção do ld_max_revisions_map...")

    for ld_item_for_rev_map in ld_documents:
        map_ld_full_name = str(ld_item_for_rev_map.get("full_name", "")).strip()
        map_ld_rev_g = str(ld_item_for_rev_map.get("revisao_atual_col_g", "")).strip()

        if not map_ld_full_name:
            continue

        ld_base_norm_map, ld_rev_norm_map, _ = _helper_get_ld_base_rev_from_ld_item(map_ld_full_name, map_ld_rev_g)

        if not ld_base_norm_map: 
            continue

        current_max_rev_str_in_map = ld_max_revisions_map.get(ld_base_norm_map, "")
        current_max_rev_tuple = get_revision_value_for_sorting(current_max_rev_str_in_map)
        new_rev_tuple = get_revision_value_for_sorting(ld_rev_norm_map)

        if new_rev_tuple > current_max_rev_tuple:
            ld_max_revisions_map[ld_base_norm_map] = ld_rev_norm_map
            if ENABLE_DETAILED_FILE_LOGGING and (TARGET_LD_FILENAME_DEBUG.lower() in ld_base_norm_map.lower()): 
                log_message(f"DEBUG_FLEX_V3 (LD_MAX_REV_MAP_UPDATE): Base '{ld_base_norm_map}', Nova Max Rev: '{ld_rev_norm_map}' (de LD: '{map_ld_full_name}')")
    
    if ENABLE_DETAILED_FILE_LOGGING:
        log_message(f"DEBUG_FLEX_V3: ld_max_revisions_map construído com {len(ld_max_revisions_map)} entradas.")
        if TARGET_LD_FILENAME_DEBUG:
            debug_base_target_map = TARGET_LD_FILENAME_DEBUG.lower()
            if debug_base_target_map in ld_max_revisions_map:
                 log_message(f"DEBUG_FLEX_V3 (LD_MAX_REV_MAP_CHECK): Para base DE DEBUG '{debug_base_target_map}', Max Rev na LD é '{ld_max_revisions_map[debug_base_target_map]}'")
            else:
                 log_message(f"DEBUG_FLEX_V3 (LD_MAX_REV_MAP_CHECK): Base DE DEBUG '{debug_base_target_map}' NÃO encontrada no ld_max_revisions_map.")

    # 2. Preparar mapa de ficheiros do Drive
    drive_files_map = {}
    unique_drive_file_dicts_for_map_build = {}
    for _, base_name_map_val_outer in files_by_discipline_drive.items():
        for _, drive_file_list_val_outer in base_name_map_val_outer.items():
            for drive_dict_val_item_outer in drive_file_list_val_outer:
                unique_drive_file_dicts_for_map_build[drive_dict_val_item_outer["name"]] = drive_dict_val_item_outer
    
    if ENABLE_DETAILED_FILE_LOGGING:
        log_message(f"DEBUG_FLEX_V3: {len(unique_drive_file_dicts_for_map_build)} ficheiros únicos do Drive para construir o mapa do Drive.")

    for original_filename_str, drive_file_info_dict in unique_drive_file_dicts_for_map_build.items():
        drive_base_norm = ""
        drive_rev_norm = "" 
        drive_parsed_info = drive_file_info_dict.get("parsed_info", {})
        
        if drive_parsed_info.get("valid") and drive_parsed_info.get("name_part_before_revision"):
            drive_base_norm = drive_parsed_info["name_part_before_revision"].lower()
            rev_from_parse = drive_parsed_info.get("revision", "") 
            drive_rev_norm = format_revision_suffix_for_comparison(rev_from_parse).lower() if rev_from_parse else ""
        else:
            drive_base_norm, _ = normalize_name_for_comparison(original_filename_str) 
            drive_base_norm = drive_base_norm.lower()
            temp_drive_base_no_ext_for_rev_extraction, _ = os.path.splitext(original_filename_str)
            rev_extraida_drive = extract_revision_from_filename(temp_drive_base_no_ext_for_rev_extraction) 
            drive_rev_norm = rev_extraida_drive.lower() if rev_extraida_drive else ""

        drive_key = (drive_base_norm, drive_rev_norm)
        if drive_key not in drive_files_map:
            drive_files_map[drive_key] = []
        drive_files_map[drive_key].append(drive_file_info_dict)

        if ENABLE_DETAILED_FILE_LOGGING and (TARGET_LD_FILENAME_DEBUG.lower() in drive_base_norm.lower()):
            log_message(f"DEBUG_FLEX_V3 (DRIVE_MAP): Arquivo Drv='{original_filename_str}', Chave Drv='{drive_key}' (Base='{drive_base_norm}', Rev='{drive_rev_norm}')")

    # 3. Processar itens da LD para correspondências
    if ENABLE_DETAILED_FILE_LOGGING:
        log_message(f"DEBUG_FLEX_V3: Processando {len(ld_documents)} itens da LD para correspondências. {len(drive_files_map)} chaves (base,rev) no mapa Drive.")

    for ld_item in ld_documents:
        ld_full_name_original = str(ld_item.get("full_name", "")).strip()
        ld_revision_g_original = str(ld_item.get("revisao_atual_col_g", "")).strip()

        if not ld_full_name_original: 
            continue
        
        ld_base_norm, ld_rev_norm, parsed_ld_fn_info_as_is = _helper_get_ld_base_rev_from_ld_item(ld_full_name_original, ld_revision_g_original)
        
        is_target_for_debug = ENABLE_DETAILED_FILE_LOGGING and \
                              (TARGET_LD_FILENAME_DEBUG.lower() in ld_base_norm.lower())
        
        ld_key = (ld_base_norm, ld_rev_norm)
        
        if is_target_for_debug:
            log_message(f"DEBUG_FLEX_V3 (LD_ITEM_PROC): Nome LD='{ld_full_name_original}', RevColG='{ld_revision_g_original}'. Chave Gerada='{ld_key}' (Base LD Final='{ld_base_norm}', Rev LD Final='{ld_rev_norm}')")
            log_message(f"DEBUG_FLEX_V3 (LD_ITEM_PARSE_DETAIL): Resultado parse_file_name para '{ld_full_name_original}': Valid={parsed_ld_fn_info_as_is.get('valid')}, NamePartBeforeRev='{parsed_ld_fn_info_as_is.get('name_part_before_revision')}', Reason='{parsed_ld_fn_info_as_is.get('reason')}'")

        matched_drive_file_dicts = drive_files_map.get(ld_key)

        if matched_drive_file_dicts:
            if is_target_for_debug:
                log_message(f"DEBUG_FLEX_V3 (MATCH): Chave LD '{ld_key}' encontrada. Ficheiros Drv: {[d['name'] for d in matched_drive_file_dicts]}")
            for matched_drive_dict in matched_drive_file_dicts:
                found_drive_original_names.add(matched_drive_dict["name"])
        else: 
            if is_target_for_debug:
                log_message(f"DEBUG_FLEX_V3 (NO_EXACT_MATCH): Chave LD '{ld_key}' não teve correspondência exata no mapa do Drive.")
            divergences["only_ld"].append(ld_item)

    # 4. Identificar 'only_drive' com a nova lógica de revisão
    if ENABLE_DETAILED_FILE_LOGGING:
        log_message(f"DEBUG_FLEX_V3: Iniciando identificação de 'only_drive' com checagem de revisão...")
        
    for drive_original_name_iter, drive_original_dict_iter in unique_drive_file_dicts_for_map_build.items():
        if drive_original_name_iter not in found_drive_original_names:
            drive_base_norm_od = ""
            drive_rev_norm_od = ""
            
            drive_parsed_info_od_loop = drive_original_dict_iter.get("parsed_info", {})
            if drive_parsed_info_od_loop.get("valid") and drive_parsed_info_od_loop.get("name_part_before_revision"):
                drive_base_norm_od = drive_parsed_info_od_loop["name_part_before_revision"].lower()
                rev_from_parse_od = drive_parsed_info_od_loop.get("revision", "")
                drive_rev_norm_od = format_revision_suffix_for_comparison(rev_from_parse_od).lower() if rev_from_parse_od else ""
            else:
                drive_base_norm_od, _ = normalize_name_for_comparison(drive_original_name_iter)
                drive_base_norm_od = drive_base_norm_od.lower()
                temp_drive_base_no_ext_od, _ = os.path.splitext(drive_original_name_iter)
                rev_extraida_drive_od = extract_revision_from_filename(temp_drive_base_no_ext_od)
                drive_rev_norm_od = rev_extraida_drive_od.lower() if rev_extraida_drive_od else ""

            ld_max_rev_for_this_base = ld_max_revisions_map.get(drive_base_norm_od)
            
            is_target_for_drive_debug = ENABLE_DETAILED_FILE_LOGGING and \
                                        (TARGET_LD_FILENAME_DEBUG.lower() in drive_base_norm_od.lower())

            if ld_max_rev_for_this_base:
                drive_rev_tuple = get_revision_value_for_sorting(drive_rev_norm_od)
                ld_max_rev_tuple_for_cmp = get_revision_value_for_sorting(ld_max_rev_for_this_base)

                if drive_rev_tuple < ld_max_rev_tuple_for_cmp:
                    if is_target_for_drive_debug:
                        log_message(f"DEBUG_FLEX_V3 (IGNORE_ONLY_DRIVE_OLD_REV): DrvFile='{drive_original_name_iter}' (Base='{drive_base_norm_od}', Rev='{drive_rev_norm_od}') é ANTERIOR à MaxRev LD='{ld_max_rev_for_this_base}'. IGNORANDO.")
                    continue 
            
            divergences["only_drive"].append(drive_original_dict_iter)
            if is_target_for_drive_debug: # Log para quando é adicionado
                log_message(f"DEBUG_FLEX_V3 (ADD_ONLY_DRIVE): DrvFile='{drive_original_name_iter}' (Base='{drive_base_norm_od}', Rev='{drive_rev_norm_od}') ADICIONADO a only_drive. MaxRev LD para esta base: '{ld_max_rev_for_this_base if ld_max_rev_for_this_base else 'NÃO EXISTE NA LD'}'")
        
    divergences["revision_mismatch"] = [] 
    
    log_message(f"INFO (perform_flexible_comparison V3): Comparação finalizada. Divergências: {len(divergences['only_ld'])} only_ld, {len(divergences['only_drive'])} only_drive.")
    return divergences

def adapt_divergences_for_report(divergences, files_by_discipline_drive):
    report_data_for_excel = {}
    drive_file_to_discipline_map = {}
    for discipline, base_name_map in files_by_discipline_drive.items():
        for _, file_info_list in base_name_map.items(): 
            for file_info_dict in file_info_list: 
                file_name_str = file_info_dict["name"] 
                if file_name_str not in drive_file_to_discipline_map:
                    drive_file_to_discipline_map[file_name_str] = discipline

    def ensure_discipline_in_report(discipline_str):
        safe_discipline_key = str(discipline_str).replace('/', '_').replace('\\', '_').strip()
        if not safe_discipline_key: safe_discipline_key = "NAO_MAPEADO"
        safe_discipline_key = safe_discipline_key[:30]
        if safe_discipline_key not in report_data_for_excel:
            report_data_for_excel[safe_discipline_key] = {
                "only_ld_items_full_info": [],
                "only_drive_original_filenames": []
            }
        return safe_discipline_key

    for ld_item_dict in divergences["only_ld"]:
        disc_ld = str(ld_item_dict.get('disciplina_ld', 'NAO_MAPEADO'))
        safe_disc_key = ensure_discipline_in_report(disc_ld)
        report_data_for_excel[safe_disc_key]["only_ld_items_full_info"].append(ld_item_dict)

    for mismatch_info in divergences["revision_mismatch"]: 
        ld_item_dict = mismatch_info['ld_item']
        ld_rev_g_formatted = mismatch_info['ld_revision_g'] 
        drive_file_info_mismatch = mismatch_info['drive_files_found_tuples'][0][0] 
        drive_fn_mismatch = drive_file_info_mismatch["name"] 
        drive_rev_mismatch_formatted = mismatch_info['drive_files_found_tuples'][0][1]

        disc_ld_mismatch = str(ld_item_dict.get('disciplina_ld', 'NAO_MAPEADO'))
        safe_disc_ld_sheet_key = ensure_discipline_in_report(disc_ld_mismatch)
        
        ld_item_copy_for_report = ld_item_dict.copy()
        ld_item_copy_for_report['_comment'] = (f"Revisão LD ({ld_rev_g_formatted or 'N/A'}) difere do Drive. "
                                               f"Arquivo Drive similar: {drive_fn_mismatch} (Rev Drive: {drive_rev_mismatch_formatted or 'N/A'})")
        report_data_for_excel[safe_disc_ld_sheet_key]["only_ld_items_full_info"].append(ld_item_copy_for_report)

        drive_file_original_discipline = drive_file_to_discipline_map.get(drive_fn_mismatch, disc_ld_mismatch)
        safe_drive_disc_sheet_key = ensure_discipline_in_report(drive_file_original_discipline)
        
        annotated_drive_fn_for_report = (f"{drive_fn_mismatch} (Rev: {drive_rev_mismatch_formatted or 'N/A'}) "
                                         f"*Relacionado a item LD com revisão divergente: {ld_item_dict.get('full_name')} (Rev LD: {ld_rev_g_formatted or 'N/A'})*")
        
        is_already_listed_as_mismatch = any(
            (isinstance(item, str) and drive_fn_mismatch in item.split(" (Rev:")[0] and "*Relacionado a item LD" in item)
            for item in report_data_for_excel[safe_drive_disc_sheet_key]["only_drive_original_filenames"]
        )
        if not is_already_listed_as_mismatch:
             report_data_for_excel[safe_drive_disc_sheet_key]["only_drive_original_filenames"].append(annotated_drive_fn_for_report)

    for drive_file_dict_only_drive in divergences["only_drive"]:
        drive_file_name_only_drive = drive_file_dict_only_drive["name"] 

        disc_drive_only = drive_file_to_discipline_map.get(drive_file_name_only_drive, "NAO_MAPEADO")
        safe_disc_drive_only_key = ensure_discipline_in_report(disc_drive_only)

        already_annotated_as_mismatch_for_this_file = any(
            isinstance(item, str) and drive_file_name_only_drive in item.split(" (Rev:")[0] and "*Relacionado a item LD" in item
            for item_list in report_data_for_excel.values()
            for item in item_list["only_drive_original_filenames"]
        )

        if not already_annotated_as_mismatch_for_this_file:
            is_plain_version_already_listed = any(
                (isinstance(item, str) and item.split(" (Rev:")[0].strip() == drive_file_name_only_drive.strip())
                for item in report_data_for_excel[safe_disc_drive_only_key]["only_drive_original_filenames"]
            )
            if not is_plain_version_already_listed:
                drive_fn_sem_ext, _ = os.path.splitext(drive_file_name_only_drive)
                rev_from_filename_only_drive = extract_revision_from_filename(drive_fn_sem_ext) 
                
                name_to_add_only_drive = f"{drive_file_name_only_drive} (Rev: {rev_from_filename_only_drive or 'N/A'})" if rev_from_filename_only_drive else drive_file_name_only_drive
                report_data_for_excel[safe_disc_drive_only_key]["only_drive_original_filenames"].append(name_to_add_only_drive)

    for disc_key_final_sort in report_data_for_excel:
        unique_drive_files_sorted = sorted(list(set(report_data_for_excel[disc_key_final_sort]["only_drive_original_filenames"])))
        report_data_for_excel[disc_key_final_sort]["only_drive_original_filenames"] = unique_drive_files_sorted

    return report_data_for_excel

def process_project_divergences(project_proc_name_gui, all_local_folders_sorted_list,
                                drive_inst, gspread_inst, gui_filters, gui_app_ref=None):
    if gui_app_ref:
        gui_app_ref.master.after(0, lambda: gui_app_ref.log_message_to_gui_text_widget("INFO: Limpando cache do Drive (se houver)..."))
    clear_drive_cache()
    if gui_app_ref:
        gui_app_ref.master.after(0, lambda: gui_app_ref.log_message_to_gui_text_widget("INFO: Cache do Drive limpo."))

    generated_reports_this_run = []
    if "TODOS OS PROJETOS" in project_proc_name_gui:
        projects_to_process_list = all_local_folders_sorted_list
        if gui_app_ref: gui_app_ref.master.after(0, lambda: gui_app_ref.update_project_progress(total=len(projects_to_process_list), value=0, text="Iniciando todos os projetos..."))
    elif project_proc_name_gui in all_local_folders_sorted_list:
        projects_to_process_list = [project_proc_name_gui]
        if gui_app_ref: gui_app_ref.master.after(0, lambda: gui_app_ref.update_project_progress(total=1, value=0, text=f"Iniciando {project_proc_name_gui[:20]}..."))
    else:
        projects_to_process_list = []

    if not projects_to_process_list:
        msg = f"Projeto '{project_proc_name_gui}' não reconhecido ou lista vazia p/ divergências."
        log_message(f"ERRO: {msg}")
        if gui_app_ref:
            gui_app_ref.master.after(0, lambda: messagebox.showerror("Erro de Projeto", msg))
            gui_app_ref.master.after(0, gui_app_ref.reenable_action_buttons)
            gui_app_ref.master.after(0, lambda: gui_app_ref.update_project_progress(text="Erro de projeto.", mode='determinate'))
        return []

    projects_processed_count = 0

    def process_single_project_logic_v01(proj_name_local_sync):
        nonlocal projects_processed_count
        if gui_app_ref:
             gui_app_ref.master.after(0, lambda: gui_app_ref.update_progress_display(text=f"Processando: {proj_name_local_sync[:30]}..."))
        log_message(f"\n--- PROCESSANDO (Lógica Flexível V3): {proj_name_local_sync} ---")
        drive_id_for_proj = get_folder_id_from_path(drive_inst, DRIVE_ROOT_FOLDER_ID, proj_name_local_sync)
        if not drive_id_for_proj:
            log_message(f"ERRO: Pasta do projeto '{proj_name_local_sync}' não encontrada no Google Drive. Pulando este projeto.")
            if gui_app_ref and hasattr(gui_app_ref, 'update_project_progress'):
                 projects_processed_count += 1
                 gui_app_ref.master.after(0, lambda: gui_app_ref.update_project_progress(value=projects_processed_count, total=len(projects_to_process_list)))
            return None
        project_code_part = proj_name_local_sync.split(' - ', 1)[0]
        cp_ld_search_match = re.match(r"([A-Z_]{0,4})?(\d{2}[_.]?\d+)", project_code_part)
        if cp_ld_search_match:
            prefix = cp_ld_search_match.group(1) or ""
            core_cp = cp_ld_search_match.group(2)
            normalized_core_cp = core_cp.replace('_', '.')
            if len(prefix) > 0 and prefix.endswith('_'): prefix = prefix[:-1]
            cp_ld_search = f"{prefix}{normalized_core_cp}"
        else:
            cp_ld_search = project_code_part.replace('_', '.')
            log_message(f"AVISO (process_single_project): Regex CP não casou com '{project_code_part}'. Usando fallback: '{cp_ld_search}'")
        if not cp_ld_search:
            log_message(f"ERRO FATAL (process_single_project): Não foi possível extrair CP de '{proj_name_local_sync}'.")
            return None
        log_message(f"INFO: Buscando LD no Drive para '{proj_name_local_sync}' usando CP base '{cp_ld_search}'.")
        ld_files_drive_list = find_project_ld_file_drive(drive_inst, drive_id_for_proj, cp_ld_search)
        raw_docs_ld = []
        if not ld_files_drive_list: log_message(f"AVISO: Nenhuma LD (GSheet ou CSV) encontrada no Drive para '{proj_name_local_sync}'.")
        else:
            ld_fn, ld_file_id, ld_mime = ld_files_drive_list[0]
            log_message(f"INFO: Processando LD do Drive: '{ld_fn}' (MIME: {ld_mime}) para {proj_name_local_sync}")
            if ld_mime == 'application/vnd.google-apps.spreadsheet':
                raw_docs_ld = read_ld_google_sheet_with_creds(gspread_inst, ld_file_id, ld_fn)
            elif ld_fn.lower().endswith(".csv") and any(m_part in ld_mime for m_part in ['text/csv', 'excel', 'octet-stream']):
                temp_dir_path = Path(TEMP_DOWNLOAD_DIR); temp_dir_path.mkdir(parents=True, exist_ok=True)
                safe_proj_name_dl = "".join(c if c.isalnum() else "_" for c in proj_name_local_sync)
                dl_path = temp_dir_path / f"TEMP_DIVERG_{safe_proj_name_dl}_{Path(ld_fn).name}"
                try:
                    log_message(f"INFO: Baixando CSV LD '{ld_fn}' para '{dl_path}'...")
                    req = drive_inst.files().get_media(fileId=ld_file_id)
                    with open(dl_path, 'wb') as f: f.write(req.execute())
                    raw_docs_ld = read_ld_csv_file(str(dl_path))
                except HttpError as e: log_message(f"Erro HTTP ao baixar CSV LD '{ld_fn}': {e}")
                except Exception as e_csv_dl: log_message(f"Erro geral ao processar CSV LD '{ld_fn}': {e_csv_dl}")
                finally:
                    if dl_path.exists():
                        try: dl_path.unlink(missing_ok=True)
                        except OSError as e_os_rem: log_message(f"AVISO: Não remover temp LD '{dl_path}': {e_os_rem}")
            else: log_message(f"AVISO: LD '{ld_fn}' tipo {ld_mime} não suportado para leitura direta.")
        docs_ld_filtered = []
        if raw_docs_ld:
            if gui_filters:
                for item in raw_docs_ld:
                    item_disc_ld = item.get('disciplina_ld', 'INDEFINIDA').upper()
                    item_sub_ld = item.get('subdisciplina_ld', '').upper()
                    item_sit_ld = item.get('situacao_ld', '').upper()
                    match_disciplina = (gui_filters.get("disciplina") is None or item_disc_ld == gui_filters["disciplina"])
                    match_subdisciplina = (gui_filters.get("subdisciplina") is None or item_sub_ld == gui_filters["subdisciplina"])
                    match_situacao = (gui_filters.get("situacao") is None or item_sit_ld == gui_filters["situacao"])
                    if match_disciplina and match_subdisciplina and match_situacao: docs_ld_filtered.append(item)
            else: docs_ld_filtered = raw_docs_ld
        else: docs_ld_filtered = []
        log_message(f"INFO: {len(raw_docs_ld if raw_docs_ld else [])} docs LD lidos, {len(docs_ld_filtered)} após filtros para '{proj_name_local_sync}'.")
        disc_map_drive = get_project_discipline_folder_map(drive_inst, drive_id_for_proj)
        drive_by_disc_with_originals = list_drive_files_recursive(drive_inst, drive_id_for_proj, disc_map_drive, proj_name_local_sync, gui_app_ref=gui_app_ref)
        log_message(f"INFO: Iniciando comparação flexível V3 para {proj_name_local_sync}")
        categorized_divergences = perform_flexible_comparison(docs_ld_filtered, drive_by_disc_with_originals)
        report_data_for_project = adapt_divergences_for_report(categorized_divergences, drive_by_disc_with_originals)
        log_message(f"INFO: Comparação flexível V3 concluída para {proj_name_local_sync}")
        report_file_path = generate_divergence_report_excel(proj_name_local_sync, report_data_for_project, output_dir=".")
        final_report_to_show_user = report_file_path
        if report_file_path:
            log_message(f"INFO: Relatório de divergências original gerado: {report_file_path}")
            cleaned_report_path = post_process_divergence_report(report_file_path, output_dir=str(Path(report_file_path).parent))
            if cleaned_report_path:
                log_message(f"INFO: Relatório pós-processado (limpo) gerado: {cleaned_report_path}")
                final_report_to_show_user = cleaned_report_path
            else:
                log_message(f"AVISO: Falha ao pós-processar o relatório. O relatório original ({report_file_path}) será mantido.")
        projects_processed_count += 1
        if gui_app_ref and hasattr(gui_app_ref, 'update_project_progress'):
            total_projects = len(projects_to_process_list)
            gui_app_ref.master.after(0, lambda p=projects_processed_count, t=total_projects, name=proj_name_local_sync: \
                                     gui_app_ref.update_project_progress(value=p, total=t, text=f"Processado: {name[:20]} ({p}/{t})"))
        return final_report_to_show_user if final_report_to_show_user else None

    if "TODOS OS PROJETOS" in project_proc_name_gui:
        for project_to_run in projects_to_process_list:
            try:
                report_result = process_single_project_logic_v01(project_to_run)
                if report_result: generated_reports_this_run.append(report_result)
            except Exception as exc_proj_run:
                log_message(f"ERRO GRAVE no processamento do projeto '{project_to_run}': {exc_proj_run}")
                traceback.print_exc()
                if gui_app_ref and hasattr(gui_app_ref, 'update_project_progress') :
                    projects_processed_count += 1
                    total_projects = len(projects_to_process_list)
                    gui_app_ref.master.after(0, lambda p=projects_processed_count, t=total_projects, name=project_to_run: \
                                             gui_app_ref.update_project_progress(value=p, total=t, text=f"Erro em {name[:15]}, continuando..."))
    else:
        try:
            report_result_single = process_single_project_logic_v01(projects_to_process_list[0])
            if report_result_single: generated_reports_this_run.append(report_result_single)
        except Exception as exc_proj_single_main:
            log_message(f"ERRO GRAVE no processamento do projeto único '{projects_to_process_list[0]}': {exc_proj_single_main}")
            traceback.print_exc()

    if gui_app_ref:
        gui_app_ref.master.after(0, gui_app_ref.reenable_action_buttons)
        gui_app_ref.master.after(0, lambda: gui_app_ref.set_generated_reports(generated_reports_this_run, "divergence"))
        completion_message = f"Processamento de divergências para {project_proc_name_gui} finalizado!"
        if not generated_reports_this_run and projects_to_process_list:
             if any(p for p in projects_to_process_list if isinstance(p, dict) and p.get('raw_docs_ld')): # Verifique se esta condição faz sentido
                completion_message += " Verifique os logs para detalhes. Pode não haver divergências ou LDs."
             else:
                completion_message += " Nenhuma LD encontrada ou processada, ou nenhuma divergência encontrada."
        elif generated_reports_this_run:
            completion_message += f" {len(generated_reports_this_run)} relatório(s) de divergência gerado(s)."
        log_message(completion_message)
        if app_gui_instance and hasattr(app_gui_instance, 'master') and app_gui_instance.master.winfo_exists():
            app_gui_instance.master.after(0, lambda msg=completion_message: messagebox.showinfo("Processamento Concluído", msg))
        gui_app_ref.master.after(0, lambda: gui_app_ref.update_project_progress(text="Pronto.", mode='determinate', value=0))
    else:
        log_message(f"\nProcessamento de divergências para {project_proc_name_gui} finalizado.")
        if generated_reports_this_run: log_message(f"Relatórios de divergência gerados: {generated_reports_this_run}")
    return generated_reports_this_run


def fetch_and_update_gui_filter_options(project_name_for_filters, drive_serv, gspread_cli, gui_app_instance_ref):
    if not gui_app_instance_ref: print("ERRO (fetch_filters): Instância GUI não fornecida."); return
    gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_progress_display(text=f"Carregando filtros para {project_name_for_filters[:25]}...", mode='indeterminate'))
    filter_data_for_gui = {'disciplinas_list_for_gui': [], 'all_subdisciplinas_list_for_gui': [], 'all_situacoes_list_for_gui': [], 'subdisciplinas_by_discipline_map': {}, 'situacoes_by_discipline_map': {}, 'situacoes_by_discipline_and_subdiscipline_map': {}}
    if "TODOS OS PROJETOS" in project_name_for_filters or not project_name_for_filters:
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_filter_options_display(filter_data_for_gui))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget("INFO: Selecione projeto específico para filtros LD."))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_progress_display(text="Selecione projeto p/ filtros.", mode='determinate'))
        return

    project_drive_id = get_folder_id_from_path(drive_serv, DRIVE_ROOT_FOLDER_ID, project_name_for_filters)
    if not project_drive_id:
        msg_no_proj_folder = f"Pasta '{project_name_for_filters}' não no Drive p/ carregar filtros LD."
        gui_app_instance_ref.master.after(0, lambda m=msg_no_proj_folder: messagebox.showerror("Erro Carregar Filtros", m))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_filter_options_display(filter_data_for_gui))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_progress_display(text="Erro carregar filtros.", mode='determinate'))
        return

    project_code_part = project_name_for_filters.split(' - ', 1)[0]
    cp_search_match = re.match(r"([A-Z_]{0,4})?(\d{2}[_.]?\d+)", project_code_part)
    if cp_search_match:
        prefix = cp_search_match.group(1) or ""
        core_cp = cp_search_match.group(2)
        normalized_core_cp = core_cp.replace('_', '.')
        if len(prefix) > 0 and prefix.endswith('_'): prefix = prefix[:-1]
        cp_search = f"{prefix}{normalized_core_cp}"
    else:
        cp_search = project_code_part.replace('_', '.')
        log_message(f"AVISO (fetch_and_update_gui_filter_options): Regex CP não casou com '{project_code_part}'. Usando fallback: '{cp_search}'")
    if not cp_search:
        log_message(f"ERRO FATAL (fetch_and_update_gui_filter_options): Não foi possível extrair CP de '{project_name_for_filters}'.")
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"ERRO: Falha ao derivar CP de '{project_name_for_filters}' para filtros."))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_filter_options_display(filter_data_for_gui))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_progress_display(text="Erro ao derivar CP para filtros.", mode='determinate'))
        return

    ld_drive_files = find_project_ld_file_drive(drive_serv, project_drive_id, cp_search)
    raw_docs_ld_for_filters = []
    if ld_drive_files:
        ld_fn, ld_id, ld_mime = ld_drive_files[0]
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"INFO: Lendo LD '{ld_fn}' (MIME: {ld_mime}, CP Buscado: {cp_search}) p/ filtros..."))
        if ld_mime == 'application/vnd.google-apps.spreadsheet': raw_docs_ld_for_filters = read_ld_google_sheet_with_creds(gspread_cli, ld_id, ld_fn)
        elif ld_fn.lower().endswith(".csv") and any(m_part in ld_mime for m_part in ['text/csv', 'excel', 'octet-stream']):
            temp_dir_filt = Path(TEMP_DOWNLOAD_DIR); temp_dir_filt.mkdir(parents=True, exist_ok=True)
            safe_proj_name_filt = "".join(c if c.isalnum() else "_" for c in project_name_for_filters)
            dl_path_filter = temp_dir_filt / f"TEMP_FILTER_{safe_proj_name_filt}_{Path(ld_fn).name}"
            try:
                req = drive_serv.files().get_media(fileId=ld_id)
                with open(dl_path_filter, 'wb') as f: f.write(req.execute())
                raw_docs_ld_for_filters = read_ld_csv_file(str(dl_path_filter))
            except HttpError as e_http_dl_filt: gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"ERRO HTTP baixar CSV LD p/ filtros: {e_http_dl_filt}"))
            except Exception as e_gen_dl_filt: gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"ERRO geral proc. CSV LD p/ filtros '{dl_path_filter}': {e_gen_dl_filt}"))
            finally:
                if dl_path_filter.exists():
                    try: dl_path_filter.unlink(missing_ok=True)
                    except OSError as e_os_rem_filt:  gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"AVISO: Erro remover temp CSV filtros '{dl_path_filter}': {e_os_rem_filt}"))
        else: gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"AVISO: Tipo LD '{ld_fn}' (MIME: {ld_mime}) não suportado p/ filtros."))
    else: gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"AVISO: Nenhuma LD p/ '{project_name_for_filters}' (CP: {cp_search}) p/ carregar filtros."))

    if not raw_docs_ld_for_filters:
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"DEBUG: Nenhum item lido da LD p/ '{project_name_for_filters}'. Filtros vazios."))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_filter_options_display(filter_data_for_gui))
        gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.update_progress_display(text="Nenhum item na LD p/ filtros.", mode='determinate'))
        return

    disciplinas_set, all_subdisciplinas_set, all_situacoes_set = set(), set(), set()
    for item in raw_docs_ld_for_filters:
        disc_ld = item.get('disciplina_ld', "").strip().upper()
        sub_ld = item.get('subdisciplina_ld', "").strip().upper()
        sit_ld = item.get('situacao_ld', "").strip().upper()
        if disc_ld:
            disciplinas_set.add(disc_ld)
            filter_data_for_gui['subdisciplinas_by_discipline_map'].setdefault(disc_ld, set()).add(sub_ld if sub_ld else "VAZIO")
            filter_data_for_gui['situacoes_by_discipline_map'].setdefault(disc_ld, set()).add(sit_ld if sit_ld else "VAZIO")
            filter_data_for_gui['situacoes_by_discipline_and_subdiscipline_map'].setdefault(disc_ld, {}).setdefault(sub_ld if sub_ld else "VAZIO", set()).add(sit_ld if sit_ld else "VAZIO")
        if sub_ld: all_subdisciplinas_set.add(sub_ld)
        if sit_ld: all_situacoes_set.add(sit_ld)

    filter_data_for_gui['disciplinas_list_for_gui'] = sorted(list(d for d in disciplinas_set if d))
    filter_data_for_gui['all_subdisciplinas_list_for_gui'] = sorted(list(s for s in all_subdisciplinas_set if s))
    filter_data_for_gui['all_situacoes_list_for_gui'] = sorted(list(s for s in all_situacoes_set if s))
    for disc_key, sub_set_val in filter_data_for_gui['subdisciplinas_by_discipline_map'].items(): filter_data_for_gui['subdisciplinas_by_discipline_map'][disc_key] = sorted(list(s for s in sub_set_val if s))
    for disc_key, sit_set_val in filter_data_for_gui['situacoes_by_discipline_map'].items(): filter_data_for_gui['situacoes_by_discipline_map'][disc_key] = sorted(list(s for s in sit_set_val if s))
    for disc_key, sub_map_val_outer in filter_data_for_gui['situacoes_by_discipline_and_subdiscipline_map'].items():
        for sub_key_inner, sit_set_val_inner in sub_map_val_outer.items(): filter_data_for_gui['situacoes_by_discipline_and_subdiscipline_map'][disc_key][sub_key_inner] = sorted(list(s for s in sit_set_val_inner if s))

    log_msg_debug = (f"DEBUG: Filtros LD processados p/ '{project_name_for_filters}': {len(filter_data_for_gui['disciplinas_list_for_gui'])}d, {len(filter_data_for_gui['all_subdisciplinas_list_for_gui'])}s, {len(filter_data_for_gui['all_situacoes_list_for_gui'])}sit.")
    gui_app_instance_ref.master.after(0, lambda fd=filter_data_for_gui: gui_app_instance_ref.update_filter_options_display(fd))
    gui_app_instance_ref.master.after(0, lambda: gui_app_instance_ref.log_message_to_gui_text_widget(f"INFO: Filtros LD p/ '{project_name_for_filters}' carregados."))

def main_with_gui():
    global drive_service, gspread_client, gmail_service, all_project_folders_local_sorted, app_gui_instance
    initial_console_log = lambda msg: print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {msg}")
    if not TKINTER_AVAILABLE:
        initial_console_log("ERRO FATAL: Tkinter não encontrado.");
        try: root_err = tk.Tk(); root_err.withdraw(); messagebox.showerror("Erro Biblioteca", "Tkinter não encontrado."); root_err.destroy()
        except: pass
        return
    if not (GOOGLE_API_LIBS_AVAILABLE and GSPREAD_AVAILABLE):
        msg_err_libs = "Libs Google API e/ou gspread não instaladas."
        initial_console_log(f"ERRO FATAL: {msg_err_libs}"); root_err = tk.Tk(); root_err.withdraw()
        messagebox.showerror("Erro Biblioteca", msg_err_libs + "\nInstale e tente novamente."); root_err.destroy()
        return
    initial_console_log("DEBUG: Iniciando main_with_gui..."); initial_console_log("DEBUG: Autenticando Google...")
    combined_scopes = list(set(SCOPES_DRIVE + SCOPES_SHEETS + SCOPES_GMAIL_SEND))
    google_api_creds = get_google_creds(TOKEN_PICKLE_FILE, CREDENTIALS_FILE, combined_scopes)
    if not google_api_creds: initial_console_log("FALHA AUTENTICAÇÃO GOOGLE."); return
    initial_console_log("DEBUG: Autenticação Google API OK."); initial_console_log("DEBUG: Construindo serviços Google API...")
    try:
        drive_service = build('drive', 'v3', credentials=google_api_creds, cache_discovery=False)
        gspread_client = gspread.authorize(google_api_creds)
        gmail_service = build('gmail', 'v1', credentials=google_api_creds, cache_discovery=False)
        initial_console_log("DEBUG: Serviços Google API OK.")
    except Exception as e_build_service:
        msg_err_serv = f"Erro construir serviços Google API: {e_build_service}"
        initial_console_log(msg_err_serv); root_err = tk.Tk(); root_err.withdraw()
        messagebox.showerror("Erro API Google", msg_err_serv); root_err.destroy()
        return
    initial_console_log(f"DEBUG: Listando pastas de: {PROJECTS_ROOT_LOCAL_SYNC}...")
    all_project_folders_raw_names = list_folders(PROJECTS_ROOT_LOCAL_SYNC)
    initial_console_log(f"DEBUG: {len(all_project_folders_raw_names) if all_project_folders_raw_names is not None else 0} pastas locais.")
    if not all_project_folders_raw_names:
        msg_no_local_folders = f"Nenhuma pasta de projeto em: '{PROJECTS_ROOT_LOCAL_SYNC}'."
        initial_console_log(msg_no_local_folders); root_err = tk.Tk(); root_err.withdraw()
        messagebox.showwarning("Nenhuma Pasta Projeto", msg_no_local_folders + "\nApp não pode continuar."); root_err.destroy()
        return
    all_project_folders_local_sorted = sorted(all_project_folders_raw_names, key=get_sort_key_for_project)
    initial_console_log(f"DEBUG: Pastas projeto ordenadas (primeiras 5): {all_project_folders_local_sorted[:5]}...")
    def process_divergences_async_starter(selected_project_name_gui, selected_filters_gui, gui_app_ref_passed):
        threading.Thread(target=process_project_divergences, args=(selected_project_name_gui, all_project_folders_local_sorted, drive_service, gspread_client, selected_filters_gui, gui_app_ref_passed), daemon=True).start()
    def fetch_ld_options_async_starter(selected_project_name_for_filters_gui, gui_app_ref_passed):
        threading.Thread(target=fetch_and_update_gui_filter_options, args=(selected_project_name_for_filters_gui, drive_service, gspread_client, gui_app_ref_passed), daemon=True).start()
    initial_console_log("DEBUG: Preparando GUI..."); root_tk = tk.Tk(); initial_console_log("DEBUG: Janela Tkinter principal OK.")
    app_gui_instance = ProjectSelectorApp(root_tk, all_project_folders_local_sorted, process_divergences_async_starter, fetch_ld_options_async_starter)
    log_message("DEBUG: Instância App GUI OK. Iniciando mainloop Tkinter..."); root_tk.mainloop()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - \n\n--- Aplicação GUI finalizada. ---")


def generate_ld_summary_report(project_name, ld_data_list, filters=None):
    if not ld_data_list:
        log_message(f"INFO (generate_ld_summary_report): Nenhum dado da LD fornecido para '{project_name}'. Relatório não gerado.")
        return None

    df_ld = pd.DataFrame(ld_data_list)

    if filters:
        log_message(f"INFO (generate_ld_summary_report): Aplicando filtros ao resumo da LD: {filters}")
        if filters.get("disciplina"):
            df_ld = df_ld[df_ld['disciplina_ld'].str.upper() == filters["disciplina"].upper()]
        if filters.get("subdisciplina"):
            df_ld = df_ld[df_ld['subdisciplina_ld'].str.upper() == filters["subdisciplina"].upper()]
        if filters.get("situacao"):
            df_ld = df_ld[df_ld['situacao_ld'].str.upper() == filters["situacao"].upper()]

    if df_ld.empty:
        log_message(f"INFO (generate_ld_summary_report): Nenhum dado da LD restou após aplicar filtros para '{project_name}'. Relatório não gerado.")
        return None

    safe_project_name = "".join(c if c.isalnum() else "_" for c in project_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("Relatorios_Resumo_LD")
    output_dir.mkdir(parents=True, exist_ok=True)
    report_filename_path = output_dir / f"Resumo_LD_{safe_project_name}_{timestamp}.xlsx"

    try:
        excel_engine_to_use = 'xlsxwriter' if XLSXWRITER_AVAILABLE else 'openpyxl'
        log_message(f"INFO (generate_ld_summary_report): Usando engine '{excel_engine_to_use}' para gerar Excel de resumo da LD para '{project_name}'. Destino: {report_filename_path}")

        with pd.ExcelWriter(str(report_filename_path), engine=excel_engine_to_use) as writer:
            df_meta_summary = pd.DataFrame([
                ("Tipo de Relatório", "Resumo da Lista de Documentos (LD)"),
                ("Projeto Analisado", project_name),
                ("Gerado em", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("Filtros Aplicados", str(filters) if filters else "Nenhum")
            ], columns=["Parâmetro", "Valor"])
            df_meta_summary.to_excel(writer, sheet_name="Metadados", index=False)

            expected_cols_summary = ['full_name', 'disciplina_ld', 'revisao_atual_col_g', 'status_col_o', 'situacao_ld', 'subdisciplina_ld']
            for col_exp in expected_cols_summary:
                if col_exp not in df_ld.columns:
                    df_ld[col_exp] = ""

            df_ld_report = df_ld.rename(columns={
                'full_name': 'Nome do Arquivo (LD)',
                'disciplina_ld': 'Disciplina (LD)',
                'revisao_atual_col_g': 'Revisão Atual (LD)',
                'status_col_o': 'Status Coluna O (LD)',
                'situacao_ld': 'Situação (LD)',
                'subdisciplina_ld': 'Subdisciplina (LD)'
            })
            report_columns_ordered = [
                'Nome do Arquivo (LD)', 'Disciplina (LD)', 'Subdisciplina (LD)',
                'Revisão Atual (LD)', 'Situação (LD)', 'Status Coluna O (LD)'
            ]
            final_report_columns = [col for col in report_columns_ordered if col in df_ld_report.columns]
            df_ld_report = df_ld_report[final_report_columns]

            df_ld_report.to_excel(writer, sheet_name="Dados_LD", index=False)

            if excel_engine_to_use == 'xlsxwriter':
                workbook_sum = writer.book
                worksheet_meta_sum = writer.sheets["Metadados"]
                header_fmt_meta_sum = workbook_sum.add_format({'bold': True, 'bg_color': '#F0F0F0', 'border':1})
                for c_num, val_meta in enumerate(df_meta_summary.columns.values):
                    worksheet_meta_sum.write(0, c_num, val_meta, header_fmt_meta_sum)
                    worksheet_meta_sum.set_column(c_num, c_num, len(str(val_meta)) + 10)

                worksheet_data_sum = writer.sheets["Dados_LD"]
                header_fmt_data_sum = workbook_sum.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center'})
                cell_fmt_wrap_sum = workbook_sum.add_format({'text_wrap': True, 'valign': 'top'})
                for c_num, val_data in enumerate(df_ld_report.columns.values):
                    worksheet_data_sum.write(0, c_num, val_data, header_fmt_data_sum)
                    col_data_vals = df_ld_report[val_data].astype(str)
                    max_len_data = col_data_vals.map(len).max() if not col_data_vals.empty else 0
                    hdr_len_data = len(str(val_data))
                    adj_width_data = max(15, min(max(max_len_data, hdr_len_data) + 5, 70))
                    if 'Nome do Arquivo' in val_data: adj_width_data = max(40, adj_width_data)
                    worksheet_data_sum.set_column(c_num, c_num, adj_width_data, cell_fmt_wrap_sum)
                worksheet_data_sum.freeze_panes(1, 0)
                if len(df_ld_report) > 0:
                    worksheet_data_sum.autofilter(0, 0, len(df_ld_report), len(df_ld_report.columns) - 1)

        log_message(f"INFO (generate_ld_summary_report): Relatório de resumo da LD gerado: {report_filename_path}")
        return str(report_filename_path)

    except PermissionError as e_perm_sum:
        log_message(f"ERRO DE PERMISSÃO ao gerar Excel de resumo LD '{report_filename_path}': {e_perm_sum}.")
        if TKINTER_AVAILABLE and app_gui_instance:
             app_gui_instance.master.after(0, lambda: messagebox.showerror("Erro de Permissão (Resumo LD)", f"Não foi possível salvar o relatório em:\n{report_filename_path}\n\nVerifique se o arquivo está aberto ou se há permissões de escrita.\n\nDetalhe: {e_perm_sum}"))
        return None
    except Exception as e_excel_sum:
        log_message(f"ERRO CRÍTICO ao gerar Excel de resumo LD '{report_filename_path}': {e_excel_sum}")
        traceback.print_exc()
        if TKINTER_AVAILABLE and app_gui_instance:
            app_gui_instance.master.after(0, lambda: messagebox.showerror("Erro ao Gerar Resumo LD", f"Ocorreu um erro inesperado ao gerar o relatório de resumo da LD:\n{e_excel_sum}"))
        return None


if __name__ == "__main__":
    libs_ok = True; critical_missing_libs_msgs = []
    if not GOOGLE_API_LIBS_AVAILABLE: critical_missing_libs_msgs.append("Bibliotecas Google API"); libs_ok = False
    if not GSPREAD_AVAILABLE: critical_missing_libs_msgs.append("Biblioteca gspread"); libs_ok = False
    if not TKINTER_AVAILABLE: critical_missing_libs_msgs.append("Biblioteca Tkinter"); libs_ok = False
    try: import pandas # type: ignore
    except ImportError: critical_missing_libs_msgs.append("Biblioteca pandas"); libs_ok=False
    try: from cachetools import TTLCache # type: ignore
    except ImportError: critical_missing_libs_msgs.append("Biblioteca cachetools"); libs_ok = False
    try: import openpyxl # type: ignore
    except ImportError: critical_missing_libs_msgs.append("Biblioteca openpyxl"); libs_ok = False
    if not XLSXWRITER_AVAILABLE: print("AVISO (Inicialização): xlsxwriter não encontrado. Excel usará openpyxl.")
    if libs_ok: main_with_gui()
    else:
        final_error_message = "Bibliotecas CRÍTICAS faltando:\n- " + "\n- ".join(critical_missing_libs_msgs) + \
                              "\n\nInstale com 'pip install <biblioteca>' e tente de novo."
        print(f"\nERRO FATAL DE DEPENDÊNCIAS:\n{final_error_message}\n")
        if TKINTER_AVAILABLE :
            temp_root_for_error_msg = None
            try:
                temp_root_for_error_msg = tk.Tk(); temp_root_for_error_msg.withdraw()
                messagebox.showerror("Erro Bibliotecas Críticas", final_error_message)
            except tk.TclError: print("ERRO: Tkinter TclError ao mostrar msg erro libs.")
            except Exception as e_final_msg: print(f"ERRO: Exceção msg final erro Tkinter: {e_final_msg}")
            finally:
                if temp_root_for_error_msg:
                    try: temp_root_for_error_msg.destroy()
                    except: pass
        else: print("AVISO: Tkinter não disponível. Não mostrou msg erro libs na GUI.")