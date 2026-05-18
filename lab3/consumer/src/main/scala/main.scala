import org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import io.circe.parser.decode
import MockData.given


object main {
  def main(array: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("broker:29092")
      .setTopics("csv")
      .setGroupId("flink-job")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val mockDataTypeInfo: TypeInformation[MockData] =
      TypeInformation.of(new TypeHint[MockData] {})

    env
      .fromSource(kafkaSource, noWatermarks(), "Kafka Source")
      .map(
        new MapFunction[String, MockData] {
          override def map(value: String): MockData =
            decode[MockData](value) match {
              case Right(v)  => v
              case Left(err) => throw new RuntimeException(s"Failed to decode JSON: $value", err)
            }
        }
      )
      .returns(mockDataTypeInfo)
      .name("Deserialization")
      .sinkTo(new SnowflakeSchemaSink(
        "jdbc:postgresql://postgres:5432/postgres",
        "postgres",
        "postgres"
      ))
      .name("Snowflake Schema Sink")

    env.execute("Job")

  }
}

