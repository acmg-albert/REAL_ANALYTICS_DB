# 从数据库读取数据并创建图表

## 项目说明

这是一个从数据库读取数据并创建图表的测试项目。项目使用Supabase作为数据库，使用FastAPI作为后端API，使用React和Plotly.js创建前端图表。

## 数据库配置

1. 创建Supabase项目
2. 在`.env`文件中配置以下信息：
   - `SUPABASE_URL`：您的Supabase项目URL
   - `SUPABASE_ANON_KEY`：公共API密钥
   - `SUPABASE_SERVICE_ROLE_KEY`：服务角色密钥（用于数据导入）

## 项目结构

```
project/
├── backend/              # FastAPI后端
│   ├── app/
│   │   ├── main.py      # 主程序
│   │   └── database.py  # 数据库操作
│   └── requirements.txt
├── frontend/            # React前端
│   ├── src/
│   │   ├── components/  # React组件
│   │   └── App.tsx     # 主应用
│   └── package.json
└── README.md
```

## 后端API

### 端点

1. `/api/data`
   - 方法：GET
   - 描述：获取图表数据
   - 参数：无
   - 返回：JSON格式的数据

2. `/api/health`
   - 方法：GET
   - 描述：健康检查
   - 参数：无
   - 返回：服务状态

## 前端图表

使用Plotly.js创建以下图表：

1. 折线图
   - 显示时间序列数据
   - 支持缩放和平移
   - 显示数据点标签

2. 柱状图
   - 显示分类数据
   - 支持排序
   - 显示数值标签

## 安装和运行

1. 后端设置：
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

2. 前端设置：
```bash
cd frontend
npm install
```

3. 配置环境变量：
   - 复制 `.env.example` 为 `.env`
   - 填写数据库配置信息

4. 运行服务：
```bash
# 后端
uvicorn app.main:app --reload

# 前端
npm start
```

## 开发说明

1. 代码风格
   - 使用Black格式化Python代码
   - 使用Prettier格式化TypeScript代码
   - 遵循ESLint规则

2. 测试
   - 使用Pytest测试后端
   - 使用Jest测试前端
   - 编写端到端测试

3. 文档
   - 使用OpenAPI文档API
   - 编写组件文档
   - 保持README更新

## 数据安全

- 所有敏感信息通过环境变量配置
- 不在代码中硬编码敏感信息
- 使用适当的CORS设置
- 实施适当的认证机制

## 部署

1. 后端部署到Render
2. 前端部署到Vercel
3. 配置环境变量
4. 设置CORS策略

## 许可证

MIT License 