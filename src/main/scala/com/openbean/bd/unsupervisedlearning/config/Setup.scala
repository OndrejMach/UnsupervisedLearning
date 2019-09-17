package com.openbean.bd.unsupervisedlearning.config

class Setup {
  def configFile: String = "application.conf"
  val settings: Settings = getSettings
  val banner: String = "\n____  __.            _____                                _________ .__                  __               .__                \n|    |/ _|           /     \\   ____ _____    ____   ______ \\_   ___ \\|  |  __ __  _______/  |_  ___________|__| ____    ____  \n|      <    ______  /  \\ /  \\_/ __ \\\\__  \\  /    \\ /  ___/ /    \\  \\/|  | |  |  \\/  ___/\\   __\\/ __ \\_  __ \\  |/    \\  / ___\\ \n|    |  \\  /_____/ /    Y    \\  ___/ / __ \\|   |  \\\\___ \\  \\     \\___|  |_|  |  /\\___ \\  |  | \\  ___/|  | \\/  |   |  \\/ /_/  >\n|____|__ \\         \\____|__  /\\___  >____  /___|  /____  >  \\______  /____/____//____  > |__|  \\___  >__|  |__|___|  /\\___  / \n        \\/                 \\/     \\/     \\/     \\/     \\/          \\/                \\/            \\/              \\//_____/  "

  private def getSettings: Settings = {
    val serviceConf = new ServiceConfig(Some(configFile))
    Settings(
      sparkAppName = Option(serviceConf.envOrElseConfig("settings.sparkAppName.value"))
      , sparkMaster = Option(serviceConf.envOrElseConfig("settings.sparkMaster.value"))
      , inputDataLocation = Option(serviceConf.envOrElseConfig("settings.inputDataLocation.value"))
      , rawSummaryFile = Option(serviceConf.envOrElseConfig("settings.rawSummaryFile.value"))
      , clusterStatsFile = Option(serviceConf.envOrElseConfig("settings.clusterStatsFile.value"))
      , crossDimensionalStatsFile = Option(serviceConf.envOrElseConfig("settings.crossDimensionalStatsFile.value"))
      , outputFile = Option(serviceConf.envOrElseConfig("settings.outputFile.value"))
      , writeMode = Option(serviceConf.envOrElseConfig("settings.writeMode.value"))
    )
  }
}