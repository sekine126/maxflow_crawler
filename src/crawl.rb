# coding: utf-8
require "nokogiri"
require "robotex"
require "pp"
require "mysql2"
require "mechanize"
require "addressable/uri"

class Crawl 

  def initialize
    @idx = 0
    @url = nil
    @depth1_urls = []
    @depth2_urls = []
    @depth2_current_urls = []
    @depth1_pages = []
    @delay = 20
    @redirect_delay = 10
    @current_depth = 0
    @robotex = Robotex.new
    @db_name = nil
    @tb_name = nil
    # Mysqlの接続
    @client = Mysql2::Client.new(:host => 'localhost', :username => 'root', :password => 'root')
    # Mechanize設定
    @agent = Mechanize.new
    @agent.user_agent_alias = 'Linux Firefox'
    @agent.follow_meta_refresh = true
  end

  def run
    # URLが設定されていなければ強制終了
    if @url == nil
      puts "Error: Require set url."
      exit
    end

    # クロール開始
    puts "Start crawl."
    crawl_depth1()
    crawl_depth2()
    puts "Finish crawl."
  end

  # スタートURLの設定
  # url string : スタートに設定するURL
  def set_url(url)
    @url = url
  end

  # 使用するデータベースの設定をする
  # db_name string: 使用するデータベースの名前、なければ自動作成する
  # tb_name string: 使用するテーブルの名前、なければ自動作成する
  def set_database(db_name, tb_name)
    @db_name = db_name
    @tb_name = tb_name
    # クエリの作成
    query = "create database if not exists #{@db_name};"
    results = @client.query(query)
    query = "create table if not exists #{@db_name}.#{@tb_name} ("
    query += "id serial primary key,"
    query += "from_url varchar(2083) not null,"
    query += "from_url_crc int unsigned not null,"
    query += "to_url varchar(2083) not null,"
    query += "to_url_crc int unsigned not null,"
    query += "created_at timestamp not null default current_timestamp,"
    query += "index(from_url_crc))"
    # クエリの実行
    results = @client.query(query)
  end

  private

  # 深さ１までのクロールをする
  def crawl_depth1
    @current_depth = 1
    # スタートURLのrobot.txt確認
    if @robotex.allowed?(@url)
      # ページ情報取得
      page = @agent.get(@url)
      # クロール情報を記憶
      @depth1_urls.push(@url)
      page.links.each do |link|
        # 自身で設定したdelayを実施
        sleep(@delay)
        # リンク先をリダイレクト
        begin
          page = link.click
        rescue Exception => e
          puts e.message
          puts page.uri
          puts e.backtrace.inspect
          next
        end
        # リンク先のrobot.txtを確認
        if @robotex.allowed?(page.uri)
          # 一度辿ったページは辿らない
          if !@depth1_urls.include?(page.uri.to_s)
            if !(page.uri.to_s.include?(@url) and page.uri.to_s[@url.size] == "#")
              # 深さ1のURLを配列に格納
              @depth1_pages.push(page)
              # クロール情報を記憶
              @depth1_urls.push(page.uri.to_s)
              # DBに保存
              save(@url, page.uri.to_s)
            end
          end
        end
      end
      # リンク数を表示
      puts "depth1 #{@depth1_urls.size}links"
    else
      puts "Error: Disallowed robot.txt in start url"
      exit
    end
  end

  # 深さ２までのクロールをする
  def crawl_depth2
    @current_depth = 2
    @depth1_pages.each do |page|
      @depth2_current_urls = []
      puts page.uri.to_s
      next if !(page.respond_to?(:links))
      parse_uri = Addressable::URI.parse(page.uri)
      domain = parse_uri.scheme+"://"+parse_uri.host
      page.links.each do |link|
        # リンクのチェック
        real_link = link.href.to_s
        next if real_link == nil
        next if real_link.size == 0
        next if real_link[0] == "#"
        next if real_link.start_with?("javascript")
        if real_link.start_with?("redirect") or real_link.start_with?("/redirect")
          # 経過表示
          puts "## redirect link"
          puts "from #{real_link}"
          # 自身で設定したdelayを実施
          sleep(@redirect_delay)
          # リンク先をリダイレクト
          begin
            next_page = link.click
          rescue Exception => e
            puts e.message
            puts page.uri
            puts e.backtrace.inspect
            next
          end
          real_link = next_page.uri.to_s
          # 経過表示
          puts "  to #{real_link}"
        elsif !(real_link.start_with?("http"))
          if real_link[0] == "/"
            real_link = domain + real_link
          else
            real_link = domain + "/" + real_link
          end
        end
        # 同じページから同じリンクには飛ばない
        if !@depth2_current_urls.include?(real_link)
          if !(real_link.include?(page.uri.to_s) and real_link[page.uri.to_s.size] == "#")
            # クロール情報を記憶
            @depth2_urls.push(real_link)
            @depth2_current_urls.push(real_link)
            # DBに保存
            save(page.uri, real_link)
          end
        end
      end
    end
    # リンク数を表示
    puts "depth2 #{@depth2_urls.size}links"
  end

  # データベースに保存する
  # from string : リンク元のURL
  # to string : リンク先のURL
  def save(from, to)
    @idx += 1
    # 画面に進捗表示
    if @current_depth == 1
      puts "## #{@idx}"
      puts "from #{from}"
      puts "  to #{to}"
    end
    # エスケープ処理
    escaped_from = from.to_s.gsub("'", "\\\\'")
    escaped_to = to.to_s.gsub("'", "\\\\'")
    # クエリを作成
    query = "insert into #{@db_name}.#{@tb_name} ("
    query += "from_url, from_url_crc, to_url, to_url_crc) "
    query += "values ("
    query += "'#{escaped_from}',"
    query += "CRC32('#{escaped_from}'),"
    query += "'#{escaped_to}',"
    query += "CRC32('#{escaped_to}'))"
    # クエリを実行
    begin
      results = @client.query(query)
    rescue Exception => e
      puts e.message
      puts query
      puts e.backtrace.inspect
    end
  end

end
