import pandas as pd
import glob

arquivos = glob.glob("dados/*.csv")

dados = pd.concat((pd.read_csv(a) for a in arquivos), ignore_index=True)