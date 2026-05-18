import com.github.tototoshi.csv._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import io.circe.syntax._

import java.io.File
import java.util.Properties
import java.math.{BigDecimal => JBigDecimal}
import scala.util.Try

object Producer {

  private def opt(s: String): Option[String] =
    Option(s).map(_.trim).filter(_.nonEmpty)

  private def required(m: Map[String, String], key: String): String =
    m.get(key).map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"Missing required field: $key"))

  private def parseInt(m: Map[String, String], key: String): Int =
    Try(required(m, key).toInt)
      .getOrElse(throw new IllegalArgumentException(s"Invalid int in field '$key': '${m.getOrElse(key, "")}'"))

  private def parseJBigDecimal(m: Map[String, String], key: String): JBigDecimal =
    Try(new JBigDecimal(required(m, key)))
      .getOrElse(throw new IllegalArgumentException(s"Invalid decimal in field '$key': '${m.getOrElse(key, "")}'"))

  private def fromMap(m: Map[String, String], internalId: Int): MockData = {
    MockData(
      internalId = internalId,

      customerPetType = required(m, "customer_pet_type"),
      customerPetName = required(m, "customer_pet_name"),
      customerPetBreed = required(m, "customer_pet_breed"),

      customerFirstName = required(m, "customer_first_name"),
      customerLastName = required(m, "customer_last_name"),
      customerAge = parseInt(m, "customer_age"),
      customerEmail = opt(m.getOrElse("customer_email", "")),
      customerCountry = required(m, "customer_country"),
      customerPostalCode = opt(m.getOrElse("customer_postal_code", "")),

      storeName = required(m, "store_name"),
      storeLocation = required(m, "store_location"),
      storeCity = required(m, "store_city"),
      storeState = opt(m.getOrElse("store_state", "")),
      storeCountry = required(m, "store_country"),
      storePhone = opt(m.getOrElse("store_phone", "")),
      storeEmail = opt(m.getOrElse("store_email", "")),

      supplierName = required(m, "supplier_name"),
      supplierContact = required(m, "supplier_contact"),
      supplierEmail = opt(m.getOrElse("supplier_email", "")),
      supplierPhone = opt(m.getOrElse("supplier_phone", "")),
      supplierAddress = required(m, "supplier_address"),
      supplierCity = required(m, "supplier_city"),
      supplierCountry = required(m, "supplier_country"),

      productName = required(m, "product_name"),
      productCategory = required(m, "product_category"),
      productPrice = parseJBigDecimal(m, "product_price"),
      productQuantity = parseInt(m, "product_quantity"),
      productWeight = parseJBigDecimal(m, "product_weight"),
      productColor = required(m, "product_color"),
      productSize = required(m, "product_size"),
      productBrand = required(m, "product_brand"),
      productMaterial = required(m, "product_material"),
      productDescription = required(m, "product_description"),
      productRating = parseJBigDecimal(m, "product_rating"),
      productReviews = parseInt(m, "product_reviews"),
      productReleaseDate = required(m, "product_release_date"),
      productExpiryDate = required(m, "product_expiry_date"),

      sellerFirstName = required(m, "seller_first_name"),
      sellerLastName = required(m, "seller_last_name"),
      sellerEmail = opt(m.getOrElse("seller_email", "")),
      sellerCountry = required(m, "seller_country"),
      sellerPostalCode = opt(m.getOrElse("seller_postal_code", "")),

      saleDate = required(m, "sale_date"),
      saleQuantity = parseInt(m, "sale_quantity"),
      saleTotalPrice = parseJBigDecimal(m, "sale_total_price"),

      petCategory = required(m, "pet_category")
    )
  }

  def main(args: Array[String]): Unit = {
    val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "csv")
    val dataDir = sys.env.getOrElse("DATA_DIR", "/app/data")

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")

    val producer = new KafkaProducer[String, String](props)

    val dir = new File(dataDir)
    val csvFiles =
      Option(dir.listFiles())
        .getOrElse(Array.empty[File])
        .filter(f => f.isFile && f.getName.endsWith(".csv"))
        .sortBy(_.getName)

    println(s"Found ${csvFiles.length} CSV files in $dataDir")

    var internalId = 1
    var totalSent = 0

    try {
      for (file <- csvFiles) {
        println(s"Processing: ${file.getName}")
        val reader = CSVReader.open(file)

        try {
          val rows = reader.allWithHeaders()

          for (row <- rows) {
            try {
              val mockData = fromMap(row, internalId)
              val json = mockData.asJson.noSpaces

              producer.send(
                new ProducerRecord[String, String](
                  topic,
                  internalId.toString,
                  json
                )
              )

              internalId += 1
              totalSent += 1

              if (totalSent % 500 == 0) {
                println(s"Sent $totalSent records...")
              }
            } catch {
              case e: Exception =>
                println(s"Error at row $internalId in file ${file.getName}: ${e.getMessage}")
                internalId += 1
            }
          }
        } finally {
          reader.close()
        }

        println(s"Done with ${file.getName}. Total sent so far: $totalSent")
      }

      producer.flush()
      println(s"Finished! Total records sent: $totalSent")
    } finally {
      producer.close()
    }
  }
}