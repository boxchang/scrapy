"# scrapy" 

抓取條件

start_urls = [
        'https://www.104.com.tw/jobs/search/list?ro=0&jobcat=2007000000&area=6003000000%2C6002000000&order=11&asc=0&page=1&mode=s&jobsource=2018indexpoc&isnew=3&sctp=M&scmin=50000&scstrict=1&scneg=0',
        'https://www.104.com.tw/jobs/search/list?ro=0&jobcat=2007000000&area=6001016000%2C6001018000%2C6001011000%2C6001012000%2C6001016000&order=11&asc=0&page=1&mode=s&jobsource=2018indexpoc&isnew=3&sctp=M&scmin=50000&scstrict=1&scneg=0',
    ]
    
抓取薪資超過五萬

地區為
6001016000	高雄市
6001018000	台中市
6001011000	台北市
6001012000	新北市
6001016000	新竹縣市
6003000000	其他亞洲
6002000000	大陸地區

類別為
2007000000	資訊軟體系統類


isnew=3 三日最新
isnew=0 本日最新
