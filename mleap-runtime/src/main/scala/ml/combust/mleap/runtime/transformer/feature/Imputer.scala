package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by mikhail on 12/18/16.
  */
case class Imputer(override val uid: String = Transformer.uniqueName("imputer"),
                   override val shape: NodeShape,
                   model: ImputerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = if(model.nullableInput) {
    (value: Option[Double]) => model(value)
  } else {
    (value: Double) => model(value)
  }
}
