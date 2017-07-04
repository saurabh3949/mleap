package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.core.tree
import ml.combust.mleap.runtime.transformer.classification.DecisionTreeClassifier
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.bundle.tree.decision.TreeSerializer
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.bundle.tree.decision.MleapNodeWrapper
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeClassifierOp extends MleapOp[DecisionTreeClassifier, DecisionTreeClassifierModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[MleapContext, DecisionTreeClassifierModel] = new OpModel[MleapContext, DecisionTreeClassifierModel] {
    override val klazz: Class[DecisionTreeClassifierModel] = classOf[DecisionTreeClassifierModel]

    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(model: Model, obj: DecisionTreeClassifierModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      TreeSerializer[tree.Node](context.file("nodes"), withImpurities = true).write(obj.rootNode)
      model.withValue("num_features", Value.long(obj.numFeatures)).
        withValue("num_classes", Value.long(obj.numClasses))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): DecisionTreeClassifierModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("tree"), withImpurities = true).read().get
      DecisionTreeClassifierModel(rootNode,
        numClasses = model.value("num_classes").getLong.toInt,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def model(node: DecisionTreeClassifier): DecisionTreeClassifierModel = node.model
}
