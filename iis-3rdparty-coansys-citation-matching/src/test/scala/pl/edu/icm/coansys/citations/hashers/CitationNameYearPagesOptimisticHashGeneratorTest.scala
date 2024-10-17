package pl.edu.icm.coansys.citations.hashers

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import pl.edu.icm.coansys.citations.data.MatchableEntity

class CitationNameYearPagesOptimisticHashGeneratorTest {

  @Test
  def generateTest(): Unit = {
    val entity = MatchableEntity.fromParameters(id = "1", author = "Jan Kowalski", year = "2002", pages = "1-5")
    val hashes = new CitationNameYearPagesOptimisticHashGenerator().generate(entity)
    assertEquals(Set("jan#2002#1#5", "kowalski#2002#1#5"), hashes.toSet)
  }
}

