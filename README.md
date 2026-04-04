おぐさんけんちゃんへ：ローカルでの動かし方

自分の環境で動かすときは、以下の手順でDBを作ってから起動してください。

①データの流し込み（初回のみ）
.gitignore で data/*.db を除外しているから 、各自でDBを作る必要があります。

Git Bash（ターミナル）で
python import_data.py
※ これでおぐさんのHTML/CSVデータが data/projects.db に格納されます。

②アプリの起動

Git Bash(ターミナル)で
streamlit run app.py

■今回の作業
・検索バグ修正: 日本語キーワードでヒットするように search_engine.py を修正
・サイドバー活用: ケロ博士の相談室として、統計やリンク集を追加
・モバイル対応: iPadやスマホでも邪魔にならないようUIを調整
