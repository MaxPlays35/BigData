import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*

object main {
  def main(array: Array[String]): Unit = {
    val config = new SparkConf()
      .setAppName("Scala job")
      .setMaster("local[*]")
      .set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")

    val sparkSession = SparkSession.builder().config(config).getOrCreate()

    import sparkSession.implicits.*

    val postgresUrl = "jdbc:postgresql://postgres:5432/postgres"
    val postgresUser = "postgres"
    val postgresPassword = "postgres"
    val postgresDriver = "org.postgresql.Driver"

    val clickHouseUrl = "jdbc:clickhouse://clickhouse:8123/reports"
    val clickHouseUser = "default"
    val clickHousePassword = "password"
    val clickHouseDriver = "com.clickhouse.jdbc.ClickHouseDriver"
    val productSalesReportTable = "reports.product_sales_report"

    val df = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.mock_data")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()

    df.select(
        $"internal_id".alias("pet_id"),
        $"customer_pet_type",
        $"customer_pet_name",
        $"customer_pet_breed"
      )
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_customer_pet")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .save()

    df.select(
        $"internal_id".alias("customer_id"),
        $"customer_first_name",
        $"customer_last_name",
        $"customer_age",
        $"customer_email",
        $"customer_country",
        $"customer_postal_code",
        $"internal_id".alias("customer_pet_id")
      )
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_customer")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .save()

    df.select(
        $"internal_id".alias("store_id"),
        $"store_name",
        $"store_location",
        $"store_city",
        $"store_state",
        $"store_country",
        $"store_phone",
        $"store_email"
      ).write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_store")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .save()

    df.select(
        $"internal_id".alias("supplier_id"),
        $"supplier_name",
        $"supplier_contact",
        $"supplier_email",
        $"supplier_phone",
        $"supplier_address",
        $"supplier_city",
        $"supplier_country"
      ).write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_supplier")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .save()

    df.select(
        $"internal_id".alias("product_id"),
        $"product_name",
        $"product_category",
        $"product_price",
        $"product_quantity",
        $"product_weight",
        $"product_color",
        $"product_size",
        $"product_brand",
        $"product_material",
        $"product_description",
        $"product_rating",
        $"product_reviews",
        $"product_release_date",
        $"product_expiry_date"
      ).write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_product")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .save()

    df.select(
        $"internal_id".alias("seller_id"),
        $"seller_first_name",
        $"seller_last_name",
        $"seller_email",
        $"seller_country",
        $"seller_postal_code"
      ).write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_seller")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .save()

    df.select(
        $"internal_id".alias("sale_id"),
        $"sale_date",
        $"internal_id".alias("sale_customer_id"),
        $"internal_id".alias("sale_seller_id"),
        $"internal_id".alias("sale_product_id"),
        $"internal_id".alias("sale_store_id"),
        $"internal_id".alias("sale_supplier_id"),
        $"sale_quantity",
        $"sale_total_price",
        $"pet_category"
      ).write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.fact_sale")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .save()

    val dimCustomer = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_customer")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()

    val dimCustomerPet = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_customer_pet")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()

    val dimProduct = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_product")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()

    val dimSeller = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_seller")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()

    val dimStore = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_store")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()

    val dimSupplier = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.dim_supplier")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()

    val factSale = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.fact_sale")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", postgresDriver)
      .load()


    // region First
    {
      val productSales = factSale
        .join(dimProduct, factSale("sale_product_id") === dimProduct("product_id"))
        .groupBy(
          dimProduct("product_id"),
          dimProduct("product_name"),
          dimProduct("product_category")
        )
        .agg(
          sum(factSale("sale_quantity")).alias("total_sales_quantity"),
          sum(factSale("sale_total_price")).alias("total_sales_revenue"),
          avg(dimProduct("product_rating")).alias("product_avg_rating"),
          max(dimProduct("product_reviews")).alias("product_reviews")
        )

      val categoryWindow = Window.partitionBy("product_category")
      val rankingWindow = Window.orderBy(
        desc("total_sales_quantity"),
        desc("total_sales_revenue"),
        asc("product_id")
      )

      val productSalesReport = productSales
        .withColumn(
          "category_total_revenue",
          sum($"total_sales_revenue").over(categoryWindow)
        )
        .withColumn("sales_rank", row_number().over(rankingWindow))
        .withColumn("is_top_10", when($"sales_rank" <= 10, lit(1)).otherwise(lit(0)))
        .select(
          $"product_id",
          $"product_name",
          $"product_category",
          $"total_sales_quantity",
          $"total_sales_revenue",
          $"product_avg_rating",
          $"product_reviews",
          $"category_total_revenue",
          $"sales_rank",
          $"is_top_10",
        )

      productSalesReport.write
        .format("clickhouse")
        .option("host", "clickhouse")
        .option("protocol", "http")
        .option("http_port", "8123")
        .option("database", "reports")
        .option("table", "sale_report")
        .option("user", clickHouseUser)
        .option("password", clickHousePassword)
        .option("order_by", "product_category, sales_rank")
        .mode(SaveMode.Overwrite)
        .save()
    }
    // endregion

    // region Second
    {
      val customerSales = factSale
        .join(dimCustomer, factSale("sale_customer_id") === dimCustomer("customer_id"))
        .groupBy(
          dimCustomer("customer_id"),
          dimCustomer("customer_first_name"),
          dimCustomer("customer_last_name"),
          dimCustomer("customer_country")
        )
        .agg(
          sum(factSale("sale_total_price")).alias("total_spent"),
          count(factSale("sale_id")).alias("total_orders"),
          avg(factSale("sale_total_price")).alias("avg_order_value"),
          sum(factSale("sale_quantity")).alias("total_items_bought")
        )

      val countryWindow = Window.partitionBy("customer_country")
      val customerRankWindow = Window.orderBy(
        desc("total_spent"),
        asc("customer_id")
      )

      val customerSalesReport = customerSales
        .withColumn(
          "country_customer_count",
          count($"customer_id").over(countryWindow)
        )
        .withColumn(
          "country_total_spent",
          sum($"total_spent").over(countryWindow)
        )
        .withColumn("spending_rank", row_number().over(customerRankWindow))
        .withColumn("is_top_10", when($"spending_rank" <= 10, lit(1)).otherwise(lit(0)))
        .select(
          $"customer_id",
          $"customer_first_name",
          $"customer_last_name",
          $"customer_country",
          $"total_spent",
          $"total_orders",
          $"avg_order_value",
          $"total_items_bought",
          $"country_customer_count",
          $"country_total_spent",
          $"spending_rank",
          $"is_top_10"
        )

      customerSalesReport.write
        .format("clickhouse")
        .option("host", "clickhouse")
        .option("protocol", "http")
        .option("http_port", "8123")
        .option("database", "reports")
        .option("table", "customer_sales_report")
        .option("user", clickHouseUser)
        .option("password", clickHousePassword)
        .option("order_by", "spending_rank")
        .mode(SaveMode.Overwrite)
        .save()
    }
    // endregion

    // region Third
    {
      val salesWithTime = factSale
        .withColumn("sale_year", year($"sale_date"))
        .withColumn("sale_month", month($"sale_date"))
        .withColumn("sale_year_month",
          date_format($"sale_date", "yyyy-MM")
        )

      val monthlySales = salesWithTime
        .groupBy("sale_year", "sale_month", "sale_year_month")
        .agg(
          sum($"sale_total_price").alias("monthly_revenue"),
          count($"sale_id").alias("monthly_orders"),
          sum($"sale_quantity").alias("monthly_items_sold"),
          avg($"sale_total_price").alias("avg_order_value")
        )

      val yearWindow = Window.partitionBy("sale_year")
      val monthOrderWindow = Window.orderBy("sale_year", "sale_month")
      val prevMonthWindow = Window.orderBy("sale_year", "sale_month")

      val timeSalesReport = monthlySales
        .withColumn(
          "yearly_revenue",
          sum($"monthly_revenue").over(yearWindow)
        )
        .withColumn(
          "yearly_orders",
          sum($"monthly_orders").over(yearWindow)
        )
        .withColumn(
          "revenue_cumulative_ytd",
          sum($"monthly_revenue").over(
            yearWindow.orderBy("sale_month")
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
          )
        )
        .withColumn(
          "prev_month_revenue",
          lag($"monthly_revenue", 1).over(prevMonthWindow)
        )
        .withColumn(
          "revenue_mom_change",
          when($"prev_month_revenue".isNotNull,
            round(
              (($"monthly_revenue" - $"prev_month_revenue") / $"prev_month_revenue") * 100,
              2
            )
          ).otherwise(lit(null))
        )
        .select(
          $"sale_year",
          $"sale_month",
          $"sale_year_month",
          $"monthly_revenue",
          $"monthly_orders",
          $"monthly_items_sold",
          $"avg_order_value",
          $"yearly_revenue",
          $"yearly_orders",
          $"revenue_cumulative_ytd",
          $"prev_month_revenue",
          $"revenue_mom_change"
        )

      timeSalesReport.write
        .format("clickhouse")
        .option("host", "clickhouse")
        .option("protocol", "http")
        .option("http_port", "8123")
        .option("database", "reports")
        .option("table", "time_sales_report")
        .option("user", clickHouseUser)
        .option("password", clickHousePassword)
        .option("order_by", "sale_year, sale_month")
        .mode(SaveMode.Overwrite)
        .save()
    }
    // endregion

    // region Fourth
    {
      val storeSales = factSale
        .join(dimStore, factSale("sale_store_id") === dimStore("store_id"))
        .groupBy(
          dimStore("store_id"),
          dimStore("store_name"),
          dimStore("store_city"),
          dimStore("store_country")
        )
        .agg(
          sum(factSale("sale_total_price")).alias("total_revenue"),
          count(factSale("sale_id")).alias("total_orders"),
          sum(factSale("sale_quantity")).alias("total_items_sold"),
          avg(factSale("sale_total_price")).alias("avg_order_value")
        )

      val storeCountryWindow = Window.partitionBy("store_country")
      val storeCityWindow = Window.partitionBy("store_country", "store_city")
      val storeRankWindow = Window.orderBy(
        desc("total_revenue"),
        asc("store_id")
      )

      val storeSalesReport = storeSales
        .withColumn(
          "country_total_revenue",
          sum($"total_revenue").over(storeCountryWindow)
        )
        .withColumn(
          "city_total_revenue",
          sum($"total_revenue").over(storeCityWindow)
        )
        .withColumn(
          "country_store_count",
          count($"store_id").over(storeCountryWindow)
        )
        .withColumn("revenue_rank", row_number().over(storeRankWindow))
        .withColumn("is_top_5", when($"revenue_rank" <= 5, lit(1)).otherwise(lit(0)))
        .select(
          $"store_id",
          $"store_name",
          $"store_city",
          $"store_country",
          $"total_revenue",
          $"total_orders",
          $"total_items_sold",
          $"avg_order_value",
          $"country_total_revenue",
          $"city_total_revenue",
          $"country_store_count",
          $"revenue_rank",
          $"is_top_5"
        )

      storeSalesReport.write
        .format("clickhouse")
        .option("host", "clickhouse")
        .option("protocol", "http")
        .option("http_port", "8123")
        .option("database", "reports")
        .option("table", "store_sales_report")
        .option("user", clickHouseUser)
        .option("password", clickHousePassword)
        .option("order_by", "revenue_rank")
        .mode(SaveMode.Overwrite)
        .save()
    }
    // endregion

    // region Fifth
    {
      val supplierSales = factSale
        .join(dimProduct, factSale("sale_product_id") === dimProduct("product_id"))
        .join(dimSupplier, factSale("sale_supplier_id") === dimSupplier("supplier_id"))
        .groupBy(
          dimSupplier("supplier_id"),
          dimSupplier("supplier_name"),
          dimSupplier("supplier_country")
        )
        .agg(
          sum(factSale("sale_total_price")).alias("total_revenue"),
          count(factSale("sale_id")).alias("total_orders"),
          sum(factSale("sale_quantity")).alias("total_items_sold"),
          avg(dimProduct("product_price")).alias("avg_product_price"),
          countDistinct(dimProduct("product_id")).alias("unique_products")
        )

      val supplierCountryWindow = Window.partitionBy("supplier_country")
      val supplierRankWindow = Window.orderBy(
        desc("total_revenue"),
        asc("supplier_id")
      )

      val supplierSalesReport = supplierSales
        .withColumn(
          "country_total_revenue",
          sum($"total_revenue").over(supplierCountryWindow)
        )
        .withColumn(
          "country_supplier_count",
          count($"supplier_id").over(supplierCountryWindow)
        )
        .withColumn("revenue_rank", row_number().over(supplierRankWindow))
        .withColumn("is_top_5", when($"revenue_rank" <= 5, lit(1)).otherwise(lit(0)))
        .select(
          $"supplier_id",
          $"supplier_name",
          $"supplier_country",
          $"total_revenue",
          $"total_orders",
          $"total_items_sold",
          $"avg_product_price",
          $"unique_products",
          $"country_total_revenue",
          $"country_supplier_count",
          $"revenue_rank",
          $"is_top_5"
        )

      supplierSalesReport.write
        .format("clickhouse")
        .option("host", "clickhouse")
        .option("protocol", "http")
        .option("http_port", "8123")
        .option("database", "reports")
        .option("table", "supplier_sales_report")
        .option("user", clickHouseUser)
        .option("password", clickHousePassword)
        .option("order_by", "revenue_rank")
        .mode(SaveMode.Overwrite)
        .save()
    }
    // endregion

    // region Sixth
    {
      val productQuality = factSale
        .join(dimProduct, factSale("sale_product_id") === dimProduct("product_id"))
        .groupBy(
          dimProduct("product_id"),
          dimProduct("product_name"),
          dimProduct("product_category"),
          dimProduct("product_rating"),
          dimProduct("product_reviews")
        )
        .agg(
          sum(factSale("sale_quantity")).alias("total_sales_quantity"),
          sum(factSale("sale_total_price")).alias("total_sales_revenue"),
          count(factSale("sale_id")).alias("total_orders")
        )

      val ratingRankHighWindow = Window.orderBy(
        desc("product_rating"),
        desc("product_reviews"),
        asc("product_id")
      )
      val ratingRankLowWindow = Window.orderBy(
        asc("product_rating"),
        desc("product_reviews"),
        asc("product_id")
      )
      val reviewsRankWindow = Window.orderBy(
        desc("product_reviews"),
        asc("product_id")
      )
      val categoryQualityWindow = Window.partitionBy("product_category")

      val productQualityReport = productQuality
        .withColumn("rating_rank_high", row_number().over(ratingRankHighWindow))
        .withColumn("rating_rank_low", row_number().over(ratingRankLowWindow))
        .withColumn("reviews_rank", row_number().over(reviewsRankWindow))
        .withColumn(
          "category_avg_rating",
          avg($"product_rating").over(categoryQualityWindow)
        )
        .withColumn(
          "category_avg_sales",
          avg($"total_sales_quantity").over(categoryQualityWindow)
        )
        .withColumn(
          "is_top_rated",
          when($"rating_rank_high" <= 10, lit(1)).otherwise(lit(0))
        )
        .withColumn(
          "is_lowest_rated",
          when($"rating_rank_low" <= 10, lit(1)).otherwise(lit(0))
        )
        .withColumn(
          "is_most_reviewed",
          when($"reviews_rank" <= 10, lit(1)).otherwise(lit(0))
        )
        .select(
          $"product_id",
          $"product_name",
          $"product_category",
          $"product_rating",
          $"product_reviews",
          $"total_sales_quantity",
          $"total_sales_revenue",
          $"total_orders",
          $"category_avg_rating",
          $"category_avg_sales",
          $"rating_rank_high",
          $"rating_rank_low",
          $"reviews_rank",
          $"is_top_rated",
          $"is_lowest_rated",
          $"is_most_reviewed"
        )

      productQualityReport.write
        .format("clickhouse")
        .option("host", "clickhouse")
        .option("protocol", "http")
        .option("http_port", "8123")
        .option("database", "reports")
        .option("table", "product_quality_report")
        .option("user", clickHouseUser)
        .option("password", clickHousePassword)
        .option("order_by", "rating_rank_high")
        .mode(SaveMode.Overwrite)
        .save()
    }
    // endregion

    sparkSession.close()
  }
}
