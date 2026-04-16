# 用户管理方案（场景 B：2-5 人小团队）

> 状态：**待实施**  
> 前置条件：当前系统功能稳定后再启动

---

## 1. 目标

- 邀请码注册（不开放公开注册）
- 登录后才能使用系统
- admin / user 两级角色
- 业务数据按 user_id 隔离

## 2. 角色定义

| 角色 | 能做什么 | 不能做什么 |
|------|---------|-----------|
| **admin** | 一切 + 管理用户 + 生成邀请码 + 触发数据下载/训练 + 查看所有人数据 | — |
| **user** | 分析个股、录入交易、查看自己的持仓/回测/信号 | 不能看别人数据、不能管理用户、不能触发系统级操作 |

## 3. 数据模型

### 3.1 User 表（新增）

```
users
├── id              INTEGER PK AUTOINCREMENT
├── username        VARCHAR(50) UNIQUE NOT NULL
├── password_hash   VARCHAR(256) NOT NULL     -- PBKDF2-SHA256
├── role            VARCHAR(10) NOT NULL DEFAULT 'user'  -- 'admin' | 'user'
├── is_active       BOOLEAN DEFAULT TRUE
├── created_at      DATETIME DEFAULT now
└── last_login      DATETIME NULLABLE
```

### 3.2 InviteCode 表（新增）

```
invite_codes
├── id              INTEGER PK AUTOINCREMENT
├── code            VARCHAR(32) UNIQUE NOT NULL  -- secrets.token_urlsafe(16)
├── created_by      INTEGER FK(users.id)
├── used_by         INTEGER FK(users.id) NULLABLE
├── expires_at      DATETIME NOT NULL
├── used_at         DATETIME NULLABLE
└── created_at      DATETIME DEFAULT now
```

### 3.3 现有表改动

需要给以下表加 `user_id` 列（`TradeRecord` 已有）：

| 表 | 当前 user_id | 改动 |
|----|-------------|------|
| `trade_records` | 已有（nullable） | 写入时绑定，查询时过滤 |
| `analysis_history` | 无 | 加 `user_id INTEGER NULLABLE INDEX` |
| `backtest_runs` | 无 | 加 `user_id INTEGER NULLABLE INDEX` |
| `signal_history` | 无 | 全局共享，不隔离（信号是模型产出，非用户个人行为） |
| `stock_daily` | 无 | 全局共享，不隔离（公共行情数据） |

## 4. 认证流程

### 4.1 密码哈希

```python
import hashlib, secrets

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return f"{salt}${h.hex()}"

def verify_password(password: str, stored: str) -> bool:
    salt, expected = stored.split("$", 1)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return h.hex() == expected
```

零额外依赖（stdlib only）。

### 4.2 JWT

- 库：`python-jose[cryptography]`
- Payload：`{"sub": user_id, "role": "admin|user", "exp": ...}`
- Secret：`config.yaml` 的 `auth.secret_key`（首次启动自动生成写入）
- 有效期：`auth.token_expire_hours`（默认 24h）

### 4.3 API 端点

```
POST /api/v1/auth/register     -- { username, password, invite_code }
POST /api/v1/auth/login        -- { username, password } → { access_token, role }
GET  /api/v1/auth/me           -- 当前用户信息（需 Bearer token）
POST /api/v1/auth/change-password  -- { old_password, new_password }
```

Admin 额外端点：

```
POST /api/v1/admin/invite      -- 生成邀请码 → { code, expires_at }
GET  /api/v1/admin/users       -- 用户列表
PUT  /api/v1/admin/users/{id}  -- 修改角色 / 禁用
```

## 5. 鉴权机制

### 5.1 FastAPI Depends

```python
# auth/deps.py

async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """解析 JWT，返回 User 对象。无效 token → 401。"""

def require_role(*roles: str):
    """返回一个 Depends，校验当前用户角色是否在 roles 中。不在 → 403。"""
```

### 5.2 端点保护策略

| 端点类别 | 保护级别 |
|---------|---------|
| `POST /auth/login`, `POST /auth/register` | 无需 token |
| `GET /health` | 无需 token |
| 所有业务 API（analysis, trading, signals, backtest） | 需要 token（任意角色） |
| `/admin/*` | 需要 token + admin 角色 |
| `/system/data-status`, `/system/config` | 需要 token + admin 角色 |

### 5.3 数据隔离实现

```python
# 查询时
query = query.filter(TradeRecord.user_id == current_user.id)

# admin 可选查所有
if current_user.role == "admin" and show_all:
    pass  # 不加 user_id 过滤
```

## 6. CLI 命令

```bash
# 首次部署：创建 admin 账户（交互式输入密码）
python main.py user create-admin

# 生成邀请码（需先有 admin）
python main.py invite create --expires 7d

# 查看用户列表
python main.py user list

# 重置密码
python main.py user reset-password <username>

# 禁用用户
python main.py user disable <username>
```

## 7. 前端改动

### 7.1 新增页面

- **登录页** (`/login`)：用户名 + 密码
- **注册页** (`/register`)：用户名 + 密码 + 邀请码

### 7.2 Token 管理

```typescript
// api/client.ts — axios interceptor
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

api.interceptors.response.use(
  (r) => r,
  (err) => {
    if (err.response?.status === 401) {
      localStorage.removeItem('access_token');
      window.location.href = '/login';
    }
    return Promise.reject(err);
  }
);
```

### 7.3 路由守卫

```typescript
// App.tsx — 未登录重定向到 /login
<Route path="/login" element={<LoginPage />} />
<Route path="/register" element={<RegisterPage />} />
<Route path="/*" element={
  isLoggedIn ? <Shell><Outlet /></Shell> : <Navigate to="/login" />
} />
```

### 7.4 设置页

- 显示当前用户名、角色
- 修改密码表单
- Admin 可见：用户列表、生成邀请码按钮

## 8. config.yaml 新增

```yaml
auth:
  secret_key: ""          # 留空则首次启动自动生成并回写
  token_expire_hours: 24  # JWT 有效期
  invite_expire_days: 7   # 邀请码默认过期天数
```

## 9. 迁移策略

1. 加 User、InviteCode 表（`init_db()` 中 `create_all` 自动处理）
2. 现有表加 `user_id` 列：SQLite `ALTER TABLE ADD COLUMN`（nullable，不影响已有数据）
3. 首次部署时 CLI 创建 admin
4. 已有的无 user_id 数据归属 admin（迁移脚本一次性 UPDATE）

## 10. 依赖新增

```
python-jose[cryptography]   # JWT 签发/验证
```

密码哈希用 stdlib，无额外依赖。

## 11. 实施顺序

| 步骤 | 内容 | 文件 |
|------|------|------|
| 1 | User + InviteCode 模型 | `storage/models.py` |
| 2 | 密码哈希 + JWT 工具 | `auth/service.py` |
| 3 | FastAPI Depends | `auth/deps.py` |
| 4 | auth API 端点 | `api/v1/endpoints/auth.py` |
| 5 | admin API 端点 | `api/v1/endpoints/admin.py` |
| 6 | 现有端点加保护 + 数据隔离 | 各 endpoints/*.py |
| 7 | CLI user / invite 命令 | `main.py` |
| 8 | 前端登录/注册/守卫/Token | 前端多文件 |
| 9 | 迁移脚本 | `storage/migrate.py` |
| 10 | 文档更新 | `COMMANDS.md` |
