package quasar.destination.avalanche

import java.util.UUID
import argonaut._, Argonaut._

final case class GoogleAuth(authId: UUID) {
  def sanitized: GoogleAuth = GoogleAuth(UUID0)
}

object GoogleAuth {
  implicit val codec: CodecJson[GoogleAuth] = 
    casecodec1(GoogleAuth.apply, GoogleAuth.unapply)("authId")
}
