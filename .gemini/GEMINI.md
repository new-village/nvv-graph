# nvv-graph Project Charter (GEMINI.md)

## 1. 言語とコミュニケーション (Language & Communication)
- **README の多言語管理 (Multilingual README Management):**
  - **EN:** Maintain `README.md` in English as the primary source (Master) and `README_ja.md` as the Japanese supplement. Ensure both are synchronized with every update.
  - **JP:** 英語版の `README.md` を原本（Master）とし、日本語版の `README_ja.md` を補完資料として作成・維持せよ。常に最新の状態を両言語で同期させること。

## 2. コード品質と開発環境 (Code Quality & Environment)
- **環境認識 (Environment Awareness):**
  - **EN:** The primary development environment is **Dev Container (Linux)** using Python 3.14.
  - **JP:** **Dev Container (Linux)**、Python 3.14 を主軸とする。

## 3. 任務遂行の規律 (Mission Discipline)
- **検証の義務 (Duty of Verification):**
  - **EN:** Include specific verification procedures in all implementation plans. Do not declare a task complete until operation is proven with tangible evidence (test results or logs).
  - **JP:** 計画には具体的な検証手順を含めよ。証拠（テスト結果やログ）をもって動作を証明するまで任務完了とは認めない。

## 4. ワークフローとブランチ戦略 (Workflow & Branch Strategy)
- **Branch-per-Task:**
  - **EN:** Create a new branch (`feature/` or `fix/`) for every task. Direct pushes to the `main` branch are strictly prohibited to maintain repository integrity.
  - **JP:** すべてのタスクにおいて必ず新しいブランチ（`feature/` または `fix/`）を作成して着手せよ。`main` ブランチへの直接プッシュは「規律違反」として厳禁とする。
- **PR & Merge:**
  - **EN:** Create a Pull Request (PR) upon task completion. Perform a rigorous self-review of