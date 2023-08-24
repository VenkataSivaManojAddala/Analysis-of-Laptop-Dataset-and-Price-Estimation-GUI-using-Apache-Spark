import scala.swing._
import scala.swing.event._
import scala.swing.Reactor
import java.awt.{BorderLayout, Font, FontMetrics, Graphics2D, Image}
import java.io._
import org.apache.spark.SparkContext

import java.awt.Color
import javax.swing.ImageIcon
import scala.swing.MenuBar.NoMenuBar.{listenTo, reactions}

object LaptopGUI {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "LaptopGUI")
    var RDD = sc.textFile("C://Users//Venu Pulagam//Desktop//laptop_data.csv")
    val Head = sc.parallelize(RDD.take(1))
    RDD = RDD.subtract(Head)

    val COST1 = RDD.map(lines => {
      val fields = lines.split(",")
      (fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(10).toInt)
    })

    val net_perc = COST1.map { case (a, b, c, d, e, f, g, h) => ((a, h * 0.15), (b, h * 0.10), (c, h * 0.20), (d, h * 0.10), (e, h * 0.10), (f, h * 0.15), (g, h * 0.05)) }


    val screensize = net_perc.map { case (a, b, c, d, e, f, g) => a }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }.sortByKey(false)

    val screensize_name = screensize.map { case (x, y) => x }.collect()
    val screensize_cost = screensize.map { case (x, y) => y }.collect()


    val screenresol = net_perc.map { case (a, b, c, d, e, f, g) => b }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }.sortByKey(false)

    val screenresol_name = screenresol.map { case (x, y) => x }.collect()
    val screenresol_cost = screenresol.map { case (x, y) => y }.collect()

    val cpu = net_perc.map { case (a, b, c, d, e, f, g) => c }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }.sortByKey(false)

    val cpu_name = cpu.map { case (x, y) => x }.collect()
    val cpu_cost = cpu.map { case (x, y) => y }.collect()

    val ram = net_perc.map { case (a, b, c, d, e, f, g) => d }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }.sortByKey(false)

    val ram_name = ram.map { case (x, y) => x }.collect()
    val ram_cost = ram.map { case (x, y) => y }.collect()

    val memory = net_perc.map { case (a, b, c, d, e, f, g) => e }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }.sortByKey(false)

    val mem_name = memory.map { case (x, y) => x }.collect()
    val mem_cost = memory.map { case (x, y) => y }.collect()

    val gpu = net_perc.map { case (a, b, c, d, e, f, g) => f }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }.sortByKey(false)

    val gpu_name = gpu.map { case (x, y) => x }.collect()
    val gpu_cost = gpu.map { case (x, y) => y }.collect()

    val os = net_perc.map { case (a, b, c, d, e, f, g) => g }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }.sortByKey(false)

    val os_name = os.map { case (x, y) => x }.collect()
    val os_cost = os.map { case (x, y) => y }.collect()

    val others = COST1.map { case (a, b, c, d, e, f, g, h) => (h * 0.15) }
    val avg = others.sum / others.count


    val ScreensizeComboBox = new ComboBox(screensize_name.toList)
    ScreensizeComboBox.preferredSize = new Dimension(500, 30)
    val ScreenresolComboBox = new ComboBox(screenresol_name.toList)
    ScreenresolComboBox.preferredSize = new Dimension(500, 30)
    val CPUComboBox = new ComboBox(cpu_name.toList)
    CPUComboBox.preferredSize = new Dimension(500, 30)
    val RAMComboBox = new ComboBox(ram_name.toList)
    RAMComboBox.preferredSize = new Dimension(500, 30)
    val memComboBox = new ComboBox(mem_name.toList)
    memComboBox.preferredSize = new Dimension(500, 30)
    val GPUComboBox = new ComboBox(gpu_name.toList)
    GPUComboBox.preferredSize = new Dimension(500, 30)
    val OSComboBox = new ComboBox(os_name.toList)
    OSComboBox.preferredSize = new Dimension(500, 30)
    val outputTextField = new TextArea(3, 25)
    outputTextField.preferredSize = new Dimension(500, 30)

    val submitButton = new Button("SEARCH")
    val font = new Font("Courier New", Font.BOLD, 18)
    val outfont = new Font("Courier New", Font.BOLD, 24)
    val labelFont = new Font("Courier New", Font.BOLD, 20)

    ScreensizeComboBox.font = font
    ScreenresolComboBox.font = font
    CPUComboBox.font = font
    RAMComboBox.font = font
    memComboBox.font = font
    GPUComboBox.font = font
    OSComboBox.font = font
    outputTextField.font = outfont
    submitButton.font = font

    val mainPanel = new GridBagPanel {
      val c = new Constraints
      c.insets = new Insets(10, 10, 10, 10)
      c.anchor = GridBagPanel.Anchor.Center
      c.fill = GridBagPanel.Fill.None
      c.weightx = 1.0
      c.weighty = 1.0

      c.gridx = 1
      c.gridy = 0

      val Screensize = new Label("Screen size")
      Screensize.font = labelFont
      Screensize.foreground = Color.WHITE
      layout(Screensize) = c
      c.gridx = 3//-1
      c.gridy = 0//-2

      layout(ScreensizeComboBox) = c
      c.gridx = 1
      c.gridy = 1


      val Screenresol = new Label("Screen resolution")
      Screenresol.font = labelFont
      Screenresol.foreground = Color.WHITE
      layout(Screenresol) = c
      c.gridx = -2
      c.gridy = 1

      layout(ScreenresolComboBox) = c
      c.gridx = 1
      c.gridy = 2

      val CPUmodel = new Label("CPU")
      CPUmodel.font = labelFont
      CPUmodel.foreground = Color.WHITE
      layout(CPUmodel) = c
      c.gridx = -2
      c.gridy = 2

      layout(CPUComboBox) = c
      c.gridx = 1
      c.gridy = 3

      val RAMmodel = new Label("RAM")
      RAMmodel.font = labelFont
      RAMmodel.foreground = Color.WHITE
      layout(RAMmodel) = c
      c.gridx = -2
      c.gridy = 3

      layout(RAMComboBox) = c
      c.gridx = 1
      c.gridy = 4

      val Memory = new Label("Memory")
      Memory.font = labelFont
      Memory.foreground = Color.WHITE
      layout(Memory) = c
      c.gridx = -2
      c.gridy = 4

      layout(memComboBox) = c
      c.gridx = 1
      c.gridy = 5

      val Graphics = new Label("Graphic card")
      Graphics.font = labelFont
      Graphics.foreground = Color.WHITE
      layout(Graphics) = c
      c.gridx = -2
      c.gridy = 5

      layout(GPUComboBox) = c
      c.gridx = 1
      c.gridy = 6

      val OStype = new Label("Operating System")
      OStype.font = labelFont
      OStype.foreground = Color.WHITE
      layout(OStype) = c
      c.gridx = -2
      c.gridy = 6

      layout(OSComboBox) = c
      c.gridx = 1
      c.gridy = 7

      val outLabel = new Label("Estimated Price")
      outLabel.font = labelFont
      outLabel.foreground = Color.WHITE
      layout(outLabel) = c
      c.gridx = -2
      c.gridy = 7
      c.gridwidth = 10

      layout(outputTextField) = c
      c.gridx = 1
      c.gridy = 8
      c.fill = GridBagPanel.Fill.None
      layout(submitButton) = c


      override protected def paintComponent(g: Graphics2D): Unit = {
        super.paintComponent(g)
        val backgroundImage = new ImageIcon("C://Users//Venu Pulagam//Desktop//534221315.jpg").getImage
        g.drawImage(backgroundImage, 0, 0, null)

      }
    }


    val frame = new MainFrame {
      title = "LAPTOP SELECTION"
      contents = new BorderPanel {

        layout(mainPanel) = BorderPanel.Position.Center
      }
      pack()
      centerOnScreen()
      maximize()
    }

    listenTo(submitButton)
    reactions += {
      case ButtonClicked(`submitButton`) =>
        val screen_size = ScreensizeComboBox.selection.item
        val screen_resolution = ScreenresolComboBox.selection.item
        val cpu_model = CPUComboBox.selection.item
        val ram_model = RAMComboBox.selection.item
        val mem_size = memComboBox.selection.item
        val gpu_type = GPUComboBox.selection.item
        val os_model = OSComboBox.selection.item

        val a = screensize_cost(screensize_name.toList.indexOf(screen_size))
        val b = screenresol_cost(screenresol_name.toList.indexOf(screen_resolution))
        val c = cpu_cost(cpu_name.toList.indexOf(cpu_model))
        val d = ram_cost(ram_name.toList.indexOf(ram_model))
        val e = mem_cost(mem_name.toList.indexOf(mem_size))
        val f = gpu_cost(gpu_name.toList.indexOf(gpu_type))
        val g = os_cost(os_name.toList.indexOf(os_model))

        val output = (a+b+c+d+e+f+g+avg)


        outputTextField.text = s"$output"


    }

    frame.visible = true
  }

}
