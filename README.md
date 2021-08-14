Batch and stream processing over various scenarios:
****************************************************************************************
1. First case   : 
    For each product :
        a)  net sales price and amount
        b)  gross sales price and amount
        c)  average sales amount of last 5 selling days
        d)  top selling location
and write results to json.


2. Second case  : 
    Find top 10 sellers on net sales in daily period and get  :
        a)  sales amount
        b)  top selling categpry
and write results to json.


3. Third case   : 
    Find price changes per each product :
        a)  if price rised display as RISE, if falled then FALL, if not any changes then SAME
and write results to json.


4. Fourth case  :
    Per location within 10 minutes windowing find :
        a)  count of distinct sellers
        b)  count of products
and write result to Kafka topic.


5. Fifth case   :
    Per each product  :
        a)  how many times is viewed together with other products
and write result to Elasticsearch.


****************************************************************************************
Case 1,2,3 and 5's solution are under batch.spark package, case 4's solution implemented by using streaming and stored under stream.spark
