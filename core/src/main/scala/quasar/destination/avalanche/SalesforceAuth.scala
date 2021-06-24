package quasar.destination.avalanche

import java.util.UUID
import argonaut._, Argonaut._

final case class SalesforceAuth(authId: UUID) {
  def sanitized: SalesforceAuth = SalesforceAuth(UUID0)
}

object SalesforceAuth {
  implicit val codec: CodecJson[SalesforceAuth] = 
    casecodec1(SalesforceAuth.apply, SalesforceAuth.unapply)("authId")
}
