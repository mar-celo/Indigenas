# Projeto: Indígenas no Serviço Público Federal (2018-2026)
## Guia de Desenvolvimento e Convenções

### 📅 Escopo Temporal
- Dados históricos de 2018 a 2026.
- Referência: Mês de dezembro (12) para 2018-2025.
- Referência: Mês de fevereiro (02) para 2026.

### 🎨 Identidade Visual (Padrão Observatório)
- **Base:** Estilo TidyTuesday com foco em anotações e storytelling.
- **Cores:**
  - Principal (Institucional): `#1B6CA8`
  - Destaque (Indígenas): `#E8593C`
  - Auxiliar (Geral/Outros): `#777777`
- **Tipografia:** Public Sans / Rawline.

### 🛠️ Tecnologias
- **Linguagem:** R (Tidyverse, gtsummary, reactable, plotly).
- **Relatório:** Quarto (.qmd) com output HTML self-contained.
- **Estatística:** Testes não-paramétricos (Mann-Whitney) para comparação de idades e funções.

### 📈 Módulos de Análise
1. **Perfil Demográfico:** KPIs, Pirâmide Etária e Escolaridade.
2. **Distribuição Geográfica:** Heatmap Leaflet e Ranking por UF/Cidade.
3. **Liderança (df_funcoes):** Lollipop charts e Dumbbell plots (Gap de representação).
4. **Tabelas Interativas:** Reactable para exploração por Órgão Superior.

### 📁 Estrutura de Pastas
- `/data`: Bases históricas (.csv ou .parquet).
- `/R`: Scripts de conexão e processamento estatístico.
- `/docs`: Local do `index.qmd` final e arquivos renderizados.
- `custom.css`: Estilização UX/UI.