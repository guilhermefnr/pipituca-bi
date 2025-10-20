# 📊 Pipituca BI - Relatórios Syndata

Sistema de Business Intelligence para geração automatizada de relatórios e análise de dados da Syndata.

## 📋 Sobre o Projeto

O Pipituca BI é uma solução desenvolvida em Python para automatizar a geração de relatórios, análise de estoque e extração de dados de produtos. O sistema se conecta ao banco de dados da Syndata e gera relatórios periódicos consolidados.

## ✨ Funcionalidades

- 📈 **Relatórios Periódicos**: Geração automatizada de relatórios com base em períodos específicos
- 📦 **Gestão de Estoque**: Análise de produtos com estoque e grades
- 🗄️ **Dump de Dados**: Geração de dumps consolidados do banco de dados
- 🔄 **Automação**: Workflows CI/CD configurados via GitHub Actions
- 🔌 **Conexão com BD**: Interface padronizada para conexão com banco de dados

## 🚀 Estrutura do Projeto

```
pipituca-bi/
├── .github/
│   └── workflows/          # Automações e CI/CD
├── dump_scripts/           # Scripts de dump de dados
├── build_period_report.py  # Gerador de relatórios periódicos
├── config.py               # Configurações do sistema
├── connect_db.py           # Gerenciador de conexões com BD
├── dump_generator.py       # Gerador de dumps consolidados
├── estoque_grade.py        # Análise de estoque por grade
├── products_with_stock.py  # Produtos com estoque disponível
├── requirements.txt        # Dependências Python
└── .gitignore             # Arquivos ignorados pelo Git
```

## 🛠️ Tecnologias

- **Python 3.x** - Linguagem principal
- **GitHub Actions** - Automação e CI/CD
- Bibliotecas conforme `requirements.txt`

## 📦 Instalação

### Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)
- Acesso ao banco de dados Syndata

### Passo a passo

1. Clone o repositório:
```bash
git clone https://github.com/guilhermefnr/pipituca-bi.git
cd pipituca-bi
```

2. Crie um ambiente virtual (recomendado):
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

4. Configure as credenciais do banco de dados em `config.py`

## 🔧 Configuração

Edite o arquivo `config.py` com as credenciais e parâmetros necessários:

```python
# Exemplo de configuração
DATABASE_HOST = "seu_host"
DATABASE_NAME = "seu_database"
DATABASE_USER = "seu_usuario"
DATABASE_PASSWORD = "sua_senha"
```

> ⚠️ **Importante**: Nunca commit credenciais sensíveis. Use variáveis de ambiente ou arquivos `.env`

## 💻 Uso

### Gerar Relatório Periódico

```bash
python build_period_report.py
```

### Analisar Estoque por Grade

```bash
python estoque_grade.py
```

### Listar Produtos com Estoque

```bash
python products_with_stock.py
```

### Gerar Dump Consolidado

```bash
python dump_generator.py
```

## 📊 Módulos Principais

### `connect_db.py`
Gerencia a conexão com o banco de dados, fornecendo interface padronizada para queries e transações.

### `build_period_report.py`
Gera relatórios baseados em períodos específicos (diário, semanal, mensal), consolidando informações de vendas e estoque.

### `dump_generator.py`
Cria dumps consolidados do banco de dados para backup ou análise offline.

### `estoque_grade.py`
Analisa o estoque considerando as grades de produtos (tamanhos, cores, variações).

### `products_with_stock.py`
Lista e filtra produtos que possuem estoque disponível para venda.

## 🔄 CI/CD

O projeto utiliza GitHub Actions para automação. Os workflows estão configurados em `.github/workflows/` e podem incluir:

- Testes automatizados
- Validação de código
- Deploy automático
- Geração periódica de relatórios

## 📝 Estrutura de Dados

O sistema trabalha com as seguintes entidades principais:

- **Produtos**: Informações de produtos cadastrados
- **Estoque**: Quantidades e grades disponíveis
- **Relatórios**: Dados consolidados por período
- **Dumps**: Backups e snapshots de dados

## 🤝 Contribuindo

Contribuições são bem-vindas! Para contribuir:

1. Faça um Fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/MinhaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona MinhaFeature'`)
4. Push para a branch (`git push origin feature/MinhaFeature`)
5. Abra um Pull Request

## 📜 Convenções de Commit

O projeto segue a convenção de commits semânticos:

- `feat:` - Nova funcionalidade
- `fix:` - Correção de bug
- `refactor:` - Refatoração de código
- `docs:` - Documentação
- `chore:` - Tarefas gerais

## 🐛 Reportando Problemas

Encontrou um bug? Abra uma [issue](https://github.com/guilhermefnr/pipituca-bi/issues) descrevendo:

- Descrição do problema
- Passos para reproduzir
- Comportamento esperado
- Comportamento atual
- Screenshots (se aplicável)

## 📄 Licença

Este projeto é proprietário da Syndata. Todos os direitos reservados.

## 👤 Autor

**Guilherme FNR**
- GitHub: [@guilhermefnr](https://github.com/guilhermefnr)

## 📞 Suporte

Para suporte e questões relacionadas ao projeto, entre em contato com a equipe de desenvolvimento da Syndata.

---

**Status do Projeto**: 🟢 Ativo e em desenvolvimento

**Última Atualização**: Outubro 2025
