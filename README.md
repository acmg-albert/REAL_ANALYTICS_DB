# Real Estate Analytics DB

## 项目说明

这是一个房地产市场数据分析项目，用于从多个数据源收集、处理和分析房地产市场数据。

## 功能特点

- 从多个数据源收集房地产市场数据
- 数据清洗和标准化
- 数据分析和可视化
- API接口提供数据访问
- 自动化数据更新

## 技术栈

- Python 3.11+
- FastAPI
- Supabase
- Pandas
- Plotly
- React + TypeScript

## 项目结构

```
REAL_ANALYTICS_DB/
├── src/                    # 源代码
│   ├── database/          # 数据库相关代码
│   ├── scripts/           # 数据处理脚本
│   └── utils/             # 工具函数
├── tests/                 # 测试代码
├── data/                  # 数据文件
├── logs/                  # 日志文件
└── docs/                  # 文档
```

## 环境配置

1. 创建Python虚拟环境：
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 配置环境变量：
   - 复制 `.env.example` 为 `.env`
   - 填写必要的配置信息（数据库URL、API密钥等）

## 数据库配置

项目使用Supabase作为数据库。您需要：

1. 创建Supabase项目
2. 在`.env`文件中配置以下信息：
   - `SUPABASE_URL`：您的Supabase项目URL
   - `SUPABASE_ANON_KEY`：公共API密钥
   - `SUPABASE_SERVICE_ROLE_KEY`：服务角色密钥（用于数据导入）

## 使用说明

1. 运行数据导入：
```bash
python -m src.scripts.import_rent_estimates
python -m src.scripts.import_vacancy_index
python -m src.scripts.import_time_on_market
```

2. 启动API服务：
```bash
uvicorn src.main:app --reload
```

3. 访问API文档：
   - http://localhost:8000/docs

## 数据安全

- 所有敏感信息（数据库URL、API密钥等）都应该通过环境变量配置
- 不要在代码中硬编码敏感信息
- 确保`.env`文件已添加到`.gitignore`
- 使用`.env.example`作为配置模板

## 开发指南

1. 代码风格
   - 使用Black格式化代码
   - 使用Flake8检查代码质量
   - 使用MyPy进行类型检查

2. 测试
   - 使用Pytest运行测试
   - 保持测试覆盖率在80%以上

3. 文档
   - 所有代码都应该有清晰的文档字符串
   - API端点应该有完整的OpenAPI文档

## 部署

项目使用Render进行部署：

1. 配置Render服务
2. 设置环境变量
3. 配置部署钩子
4. 启用自动部署

## 贡献指南

1. Fork项目
2. 创建功能分支
3. 提交更改
4. 创建Pull Request

## 许可证

MIT License 