package com.github.fabianmurariu.esoffline

import com.optimaize.langdetect.i18n.LdLocale
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories

object LanguageSupport {

  val supportedLang: Map[String, String] = Seq("arabic" -> "ar", "basque" -> "eu",
    "bengali" -> "bn", "brazilian" -> "pt", "bulgarian" -> "bg", "catalan" -> "ca",
    "czech" -> "cs", "danish" -> "da", "dutch" -> "nl",
    "english" -> "en", "finnish" -> "fi", "french" -> "fr",
    "german" -> "de", "greek" -> "el", "hindi" -> "hi", "hungarian" -> "hu",
    "indonesian" -> "id", "irish" -> "ga", "italian" -> "it", "latvian" -> "lv",
    "lithuanian" -> "lt", "norwegian" -> "no", "persian" -> "fa", "portuguese" -> "pt",
    "romanian" -> "ro", "russian" -> "ru", "spanish" -> "es",
    "swedish" -> "sv", "turkish" -> "tr", "thai" -> "th", "smartcn" -> "zh",
    "smartcn" -> "cn", "kuromoji" -> "ja", "nori" -> "ko").map(_.swap).toMap

  def langDetect: String => Option[LdLocale] = {
    //load all languages://load all languages:

    val languageProfiles = new LanguageProfileReader().readAllBuiltIn

    //build language detector:
    val languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard).withProfiles(languageProfiles).build

    //create a text object factory
    val textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText

    { text: String =>
      val textObject = textObjectFactory.forText(text)
      val maybeLang = languageDetector.detect(textObject)
      if (maybeLang.isPresent) Some(maybeLang.get())
      else None
    }
  }

}
