
//因为某些原因所以我的DF是从本地文件加载的，实际使用的时候需要从数据库加载
//模型生成文件无法转换为pmml，因为spark本身没有获得支持
/*


import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression

val date = java.time.LocalDate.now.toString

val path = "/data/bd/huangjiawei/sms_model/sms_test.txt" 
val sentenceData = sc.textFile(path,20).map(_.split(",")).map(x => (x(0).toDouble, x(1))).toDF().toDF("label", "sentence")

val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("rawFeatures").setNumFeatures(1000)
val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")
val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)//.setInputCol(idf.getOutputCol).setOutputCol("regression")

val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, lr))

val model = pipeline.fit(sentenceData)

model.save(s"/data/bd/huangjiawei/sms_model/sms_test_model_${date}" )



//加载模型并应用

import org.apache.spark.ml.Pipeline
val date = java.time.LocalDate.now.toString
val savedModel = Pipeline.load(s"/data/bd/huangjiawei/sms_model/sms_test_model_${date}")

val path = "/data/bd/huangjiawei/sms_model/sms_test.txt" 
val testDataFrame = sc.textFile(path,20).map(_.split(",")).map(x => (x(0).toDouble, x(1))).toDF().toDF("label", "sentence")

val test_result = savedModel.transform(testDataFrame)
test_result.select("label", "probability", "prediction").
  collect().
  foreach { case Row(label: Double, prob: Vector, prediction: Double) =>
    println(s"($label) --> prob=$prob, prediction=$prediction")
  }




//评价

import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector
val date = java.time.LocalDate.now.toString
val savedModel = PipelineModel.load(s"/data/bd/huangjiawei/sms_model/sms_test_model.txt")

val path = "/data/bd/huangjiawei/sms_model/sms_test.txt" 
val testDataFrame = sc.textFile(path,20).map(_.split(",")).map(x => (x(0).toDouble, x(1))).toDF().toDF("label", "sentence")
val test_result = savedModel.transform(testDataFrame)
val test_m = test_result.select("label", "probability", "prediction").map{ 
    case Row(label: Double, prob: Vector, prediction: Double) =>
        (label, prob(1), prediction)
    }.rdd

val prediction = test_m.map(x=>(x._2, x._1))
val metrics = new BinaryClassificationMetrics(prediction)

// Precision by threshold
val precision = metrics.precisionByThreshold
precision.foreach { case (t, p) =>
  println(s"Threshold: $t, Precision: $p")
}

// Recall by threshold
val recall = metrics.recallByThreshold
recall.foreach { case (t, r) =>
  println(s"Threshold: $t, Recall: $r")
}

// Precision-Recall Curve
val PRC = metrics.pr

// F-measure
val f1Score = metrics.fMeasureByThreshold
f1Score.foreach { case (t, f) =>
  println(s"Threshold: $t, F-score: $f, Beta = 1")
}

val beta = 0.5
val fScore = metrics.fMeasureByThreshold(beta)
f1Score.foreach { case (t, f) =>
  println(s"Threshold: $t, F-score: $f, Beta = 0.5")
}

// AUPRC
val auPRC = metrics.areaUnderPR
println("Area under precision-recall curve = " + auPRC)

// Compute thresholds used in ROC and PR curves
val thresholds = precision.map(_._1)

// ROC Curve
val roc = metrics.roc

// AUROC
val auROC = metrics.areaUnderROC
println("Area under ROC = " + auROC)*/
