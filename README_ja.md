# nvv-graph

**DuckDB/DuckPGQ を活用した高性能グラフ探索バックエンド**

`nvv-graph` は、DuckDB (DuckPGQ 拡張) を利用した高速なグラフ探索と、SQLite による認証・監査ログ管理を組み合わせた、モダンな FastAPI バックエンドサービスです。

## 🚀 プロジェクトの目的 (Project Goal)

- **高速グラフ探索**: DuckDB/DuckPGQ を活用し、機密データの複雑なグラフクエリを低レイテンシで実行します。
- **データ主権 (Data Sovereignty)**: 外部のマネージド DB サービスへの依存を排除し、自前環境内でデータを完全に管理します。
- **ポータブルかつ高セキュア**: Cloud Run やオンプレミス Docker など、あらゆる環境で展開可能な「Security First」な設計を採用しています。

## 🏗 アーキテクチャ (Architecture)

### 技術スタック (Technology Stack)
- **言語**: Python 3.14
- **Web フレームワーク**: FastAPI
- **メインエンジン**: DuckDB (DuckPGQ 拡張) - Parquet ファイルの直接参照
- **認証/監査ストア**: SQLite (SQLAlchemy 経由で管理)
- **コンテナ化**: Dev Container / Docker

### 主な機能 (Key Features)
- **構成可能なデータソース**: パスやスキーマ定義は `config/sources.yaml` で外部定義し、コードから分離しています。
- **セキュアな認証**: OAuth2 + JWT による認証機能を搭載しています。
- **永続化戦略**: 計算コンテナをステートレスに保ちつつ、認証・監査ログの永続化には外部ボリュームマウントを使用します。

## 🛠 始め方 (Getting Started)

### 前提条件 (Prerequisites)
- **Dev Container**: 本プロジェクトは Dev Container 内での開発を前提としています。Docker および VS Code (Dev Containers 拡張機能付き) が必要です。

### インストールと実行 (Installation & Running)
1. プロジェクトを VS Code で開きます。
2. プロンプトが表示されたら "Reopen in Container" を選択します。
3. Python 3.14 環境と依存関係が自動的にセットアップされます。
4. API サーバーを起動します:
   ```bash
   uvicorn src.main:app --reload
   ```

## 📡 API エンドポイント (API Endpoints)

### GET `/node/{node_type}/{node_id}`
指定された種別 (`node_type`) と ID (`node_id`) に一致するノード情報を取得します。

- **Parameters**:
  - `node_type` (path): `config/icij.yaml` の `label` で定義されたノード種別 (例: `Officer`, `Entity`)
  - `node_id` (path): ノードのユニーク ID (例: `12000001`)
- **Response**:
  ```json
  {
    "count": 1,
    "data": {
      "node_id": 12000001,
      "name": "Target Name",
      ...
    }
  }
  ```
- **Not Found**:
  対象レコードが存在しない場合、`count: 0, data: null` を返却します。
  ```json
  {
    "count": 0,
    "data": null
  }
  ```
