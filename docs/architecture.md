# yata-graph Basic Architecture Specification

## 1. プロジェクトの目的 (Project Goal)
- 機密性の高いデータを対象とした、DuckDB/DuckPGQ による高速なグラフ探索バックエンドを構築する。
- 外部マネージド DB サービスへの依存を排除し、データの主権を自前環境内に保持する。
- Cloud Run やオンプレミス Docker 等、多様な環境での展開を想定した「ポータブルかつ高セキュア」な設計とする。

## 2. データ構造と管理 (Data Structure & Management)
- **データソース管理:**
- **データソース管理:**
  - `data/nodes.parquet` および `data/edges.parquet` を直接参照する。configファイルによる定義は廃止。
- **データベース構成:**
  - **Main Engine:** DuckDB (DuckPGQ 拡張) による高速分析。
  - **Auth/Audit Store:** MVPでは **SQLite** を使用。
  - **Persistence Strategy:**
    - 認証・監査ログの永続化には、ホスト環境から提供される「外部ボリューム・マウント」を活用する。
    - SQLite の整合性を維持するため、マウントされたファイルに対する同時書き込みを制限する「単一インスタンス運用」を原則とする。

## 3. アプリケーション仕様 (Application Spec)
- **Framework:** FastAPI (Python 3.14).
- **Database Abstraction:**
  - **SQLAlchemy (ORM)** を採用。SQLite への依存を抽象化し、将来的な PostgreSQL 等へのデータベース切り替えを容易にする。
- **Authentication & Authorization:**
  - OAuth2 + JWT による認証。SQLAlchemy を通じてユーザー情報および監査ログを管理する。
- **主要 API Endpoints:**
  - `GET /nodes/{node_type}/{id}`
  - `GET /nodes/{node_type}/{id}/neighbors`
  - `POST /graph/search`
  - `POST /auth/login` / `GET /auth/me` / `DELETE /auth/logout`

## 4. 運用・セキュリティ (Ops & Security)
- **機密管理:** 全データおよび DB ファイルはリポジトリに含めず、`.gitignore` で厳格に管理する。
- **環境のポータビリティ:** Dev Container 準拠の開発環境を維持し、開発・テスト・本番（クラウド/オンプレ）間の差分を最小化する。
- **スケーラビリティ制限:** ネットワーク共有ストレージ経由で SQLite を使用する場合の破損リスクを考慮し、アプリケーションレイヤーでの排他制御またはインスタンス制限を適用する。