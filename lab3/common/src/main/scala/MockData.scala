import io.circe.{Decoder, Encoder, HCursor}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import java.math.BigDecimal as JBigDecimal

final case class MockData(
                           internalId: Int,
                           customerPetType: String,
                           customerPetName: String,
                           customerPetBreed: String,
                           customerFirstName: String,
                           customerLastName: String,
                           customerAge: Int,
                           customerEmail: Option[String],
                           customerCountry: String,
                           customerPostalCode: Option[String],
                           storeName: String,
                           storeLocation: String,
                           storeCity: String,
                           storeState: Option[String],
                           storeCountry: String,
                           storePhone: Option[String],
                           storeEmail: Option[String],
                           supplierName: String,
                           supplierContact: String,
                           supplierEmail: Option[String],
                           supplierPhone: Option[String],
                           supplierAddress: String,
                           supplierCity: String,
                           supplierCountry: String,
                           productName: String,
                           productCategory: String,
                           productPrice: JBigDecimal,
                           productQuantity: Int,
                           productWeight: JBigDecimal,
                           productColor: String,
                           productSize: String,
                           productBrand: String,
                           productMaterial: String,
                           productDescription: String,
                           productRating: JBigDecimal,
                           productReviews: Int,
                           productReleaseDate: String,
                           productExpiryDate: String,
                           sellerFirstName: String,
                           sellerLastName: String,
                           sellerEmail: Option[String],
                           sellerCountry: String,
                           sellerPostalCode: Option[String],
                           saleDate: String,
                           saleQuantity: Int,
                           saleTotalPrice: JBigDecimal,
                           petCategory: String
                         )

object MockData {
  given Decoder[JBigDecimal] =
    Decoder.decodeBigDecimal.map(_.bigDecimal)

  given Decoder[MockData] = deriveDecoder[MockData]

  given Encoder[MockData] = deriveEncoder[MockData]
}