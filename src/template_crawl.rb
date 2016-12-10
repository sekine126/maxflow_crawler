# coding: utf-8
# クローラー本体のファイルを読み込み
require "./src/crawl.rb"

# コマンドオプション'd:'で日付を指定
# 例：bundle exec ruby src/template_crawl.rb -d 20160809
params = ARGV.getopts('d:')
if params["d"] == nil
  puts "Error: Please set -d date option."
  exit(1)
end
if params["d"] != nil && params["d"].size != 8 
  puts "Error: -d is date. e.g. 20150214"
  exit(1)
end

# クロール
crawl = Crawl.new()
#########################################################
# スタートページのURLをここに貼り付ける
#url = "http://news.yahoo.co.jp/"
# データベースの名前とテーブル名を設定
# データベース名：template_crawl
# テーブル名：link_20160809
crawl.set_database("template_crawl","link1_#{params['d']}")
#########################################################
crawl.set_url(url)
crawl.run
