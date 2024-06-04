
# 出力ファイル
DB  <- 'weather.duckdb' # データベース名
F.O <- 'weather.csv'    # CSVファイル名

# 気象観測所
site <- data.frame(
  id   = 47662,   # 番号
  name = 'Tokyo') # 名称（データベースのテーブル名として使う）

# 対象日時（テーブル取得のためのURLに適用する日時）
t.fr <- as.POSIXlt('2021-12-30')
t.to <- as.POSIXlt('2022-01-01')
ts   <- as.POSIXlt(seq(t.fr, t.to, by = 'days'))


library(duckdb)
library(rvest)

# 既存テーブル削除（必要に応じて実施）
con <- dbConnect(duckdb(), DB) # 「test」というデータベース接続／新規作成
dbSendQuery(con, paste('DROP TABLE IF EXISTS', site$name))

for(i in 1:3)
  {year  <- 1900 + ts[i]$year
  month <- 1 + ts[i]$mon
  day   <- ts[i]$mday
  url <- paste0('https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?prec_no=44&block_no=', site$id, '&year=', year, '&month=', month, '&day=', day, '&view=')
  cat('URL:', url, fill = T)

  
  read_html(url) |> html_table() -> tbl
  tbl
  
  d0 <- as.data.frame(tbl[[5]])
  str(d0)
  
  # 日時整形
  hour <- d0[-1, '時'] # 1列目は時刻1～24（-1:一行目は不要なため削除）
  # コンピュータの世界(POSIX準拠)では24時は存在しないので0～23にする必要がある。
  # コンピュータ上では24時は翌日の日付になる。
  datetime <- as.POSIXlt(paste(ts[i], hour))        # 例）2022-08-10 24
  # 自動で時刻が0～23に変換される。
  
  # 書込用テーブル作成
  d1 <- data.frame(site.id   = as.integer(site$id), # 整数型
                   site.name = site$name,
                   datetime  = paste(datetime),
                   temp      = as.double(d0[-1, 5]), # 倍精度浮動小数点型
                   wind      = d0[-1, 10])
  str(d1)


  dbWriteTable(con, site$name, d1, append = T)
  Sys.sleep(runif(1, min = 1, max = 2))
}

# データ選択（ちゃんと保存されたか確認すること）
res <- dbSendQuery(con, 'SELECT * FROM Tokyo')

# 選択結果取得
dbFetch(res)

# 選択結果解放
dbClearResult(res)
# データベース接続解除 
dbDisconnect(con, shutdown = T)
