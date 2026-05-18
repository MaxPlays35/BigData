import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import java.sql.{Connection, DriverManager, PreparedStatement, Date}
import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SnowflakeSchemaSinkWriter(
                                 jdbcUrl: String,
                                 username: String,
                                 password: String
                               ) extends SinkWriter[MockData] {

  private val inputDateFormat = DateTimeFormatter.ofPattern("M/d/yyyy")

  private def parseSqlDate(raw: String): Date = {
    Date.valueOf(LocalDate.parse(raw.trim, inputDateFormat))
  }

  private val connection: Connection = {
    val conn = DriverManager.getConnection(jdbcUrl, username, password)
    conn.setAutoCommit(false)
    conn
  }

  private val petStmt: PreparedStatement = connection.prepareStatement(
    """INSERT INTO dim_customer_pet (pet_id, customer_pet_type, customer_pet_name, customer_pet_breed)
       VALUES (?, ?, ?, ?)
       ON CONFLICT (pet_id) DO NOTHING"""
  )
  private val customerStmt: PreparedStatement = connection.prepareStatement(
    """INSERT INTO dim_customer (customer_id, customer_first_name, customer_last_name,
       customer_age, customer_email, customer_country, customer_postal_code, customer_pet_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (customer_id) DO NOTHING"""
  )
  private val storeStmt: PreparedStatement = connection.prepareStatement(
    """INSERT INTO dim_store (store_id, store_name, store_location, store_city,
       store_state, store_country, store_phone, store_email)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (store_id) DO NOTHING"""
  )
  private val supplierStmt: PreparedStatement = connection.prepareStatement(
    """INSERT INTO dim_supplier (supplier_id, supplier_name, supplier_contact, supplier_email,
       supplier_phone, supplier_address, supplier_city, supplier_country)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (supplier_id) DO NOTHING"""
  )
  private val productStmt: PreparedStatement = connection.prepareStatement(
    """INSERT INTO dim_product (product_id, product_name, product_category, product_price,
       product_quantity, product_weight, product_color, product_size, product_brand,
       product_material, product_description, product_rating, product_reviews,
       product_release_date, product_expiry_date)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (product_id) DO NOTHING"""
  )
  private val sellerStmt: PreparedStatement = connection.prepareStatement(
    """INSERT INTO dim_seller (seller_id, seller_first_name, seller_last_name,
       seller_email, seller_country, seller_postal_code)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT (seller_id) DO NOTHING"""
  )
  private val saleStmt: PreparedStatement = connection.prepareStatement(
    """INSERT INTO fact_sale (sale_id, sale_date, sale_customer_id, sale_seller_id,
       sale_product_id, sale_store_id, sale_supplier_id, sale_quantity,
       sale_total_price, pet_category)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
  )

  override def write(value: MockData, context: SinkWriter.Context): Unit = {
    try {
      val id = value.internalId

      petStmt.setInt(1, id)
      petStmt.setString(2, value.customerPetType)
      petStmt.setString(3, value.customerPetName)
      petStmt.setString(4, value.customerPetBreed)
      petStmt.executeUpdate()

      customerStmt.setInt(1, id)
      customerStmt.setString(2, value.customerFirstName)
      customerStmt.setString(3, value.customerLastName)
      customerStmt.setInt(4, value.customerAge)
      setOptional(customerStmt, 5, value.customerEmail)
      customerStmt.setString(6, value.customerCountry)
      setOptional(customerStmt, 7, value.customerPostalCode)
      customerStmt.setInt(8, id)
      customerStmt.executeUpdate()

      storeStmt.setInt(1, id)
      storeStmt.setString(2, value.storeName)
      storeStmt.setString(3, value.storeLocation)
      storeStmt.setString(4, value.storeCity)
      setOptional(storeStmt, 5, value.storeState)
      storeStmt.setString(6, value.storeCountry)
      setOptional(storeStmt, 7, value.storePhone)
      setOptional(storeStmt, 8, value.storeEmail)
      storeStmt.executeUpdate()

      supplierStmt.setInt(1, id)
      supplierStmt.setString(2, value.supplierName)
      supplierStmt.setString(3, value.supplierContact)
      setOptional(supplierStmt, 4, value.supplierEmail)
      setOptional(supplierStmt, 5, value.supplierPhone)
      supplierStmt.setString(6, value.supplierAddress)
      supplierStmt.setString(7, value.supplierCity)
      supplierStmt.setString(8, value.supplierCountry)
      supplierStmt.executeUpdate()

      productStmt.setInt(1, id)
      productStmt.setString(2, value.productName)
      productStmt.setString(3, value.productCategory)
      productStmt.setBigDecimal(4, value.productPrice)
      productStmt.setInt(5, value.productQuantity)
      productStmt.setBigDecimal(6, value.productWeight)
      productStmt.setString(7, value.productColor)
      productStmt.setString(8, value.productSize)
      productStmt.setString(9, value.productBrand)
      productStmt.setString(10, value.productMaterial)
      productStmt.setString(11, value.productDescription)
      productStmt.setBigDecimal(12, value.productRating)
      productStmt.setInt(13, value.productReviews)
      productStmt.setDate(14, parseSqlDate(value.productReleaseDate))
      productStmt.setDate(15, parseSqlDate(value.productExpiryDate))
      productStmt.executeUpdate()

      sellerStmt.setInt(1, id)
      sellerStmt.setString(2, value.sellerFirstName)
      sellerStmt.setString(3, value.sellerLastName)
      setOptional(sellerStmt, 4, value.sellerEmail)
      sellerStmt.setString(5, value.sellerCountry)
      setOptional(sellerStmt, 6, value.sellerPostalCode)
      sellerStmt.executeUpdate()

      saleStmt.setInt(1, id)
      saleStmt.setDate(2, parseSqlDate(value.saleDate))
      saleStmt.setInt(3, id)
      saleStmt.setInt(4, id)
      saleStmt.setInt(5, id)
      saleStmt.setInt(6, id)
      saleStmt.setInt(7, id)
      saleStmt.setInt(8, value.saleQuantity)
      saleStmt.setBigDecimal(9, value.saleTotalPrice)
      saleStmt.setString(10, value.petCategory)
      saleStmt.executeUpdate()

      connection.commit()
    } catch {
      case e: Exception =>
        connection.rollback()
        throw new RuntimeException(s"Failed to insert id=${value.internalId}", e)
    }
  }

  private def setOptional(stmt: PreparedStatement, idx: Int, value: Any): Unit = {
    value match {
      case Some(v: String) if v.nonEmpty =>
        stmt.setString(idx, v)

      case Some(_) | None | null =>
        stmt.setNull(idx, java.sql.Types.VARCHAR)

      case v: String if v.nonEmpty =>
        stmt.setString(idx, v)

      case _ =>
        stmt.setNull(idx, java.sql.Types.VARCHAR)
    }
  }

  override def flush(endOfInput: Boolean): Unit = ()

  override def close(): Unit = {
    petStmt.close()
    customerStmt.close()
    storeStmt.close()
    supplierStmt.close()
    productStmt.close()
    sellerStmt.close()
    saleStmt.close()
    if (connection != null) connection.close()
  }
}

class SnowflakeSchemaSink(
                           jdbcUrl: String,
                           username: String,
                           password: String
                         ) extends Sink[MockData] {

  override def createWriter(context: WriterInitContext): SinkWriter[MockData] =
    new SnowflakeSchemaSinkWriter(jdbcUrl, username, password)
}