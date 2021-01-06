import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.util.logging.LogManager;

public class Lab_3 {
    private final static String logConfig = "log.config";   // - имя конфигурационного файла для логгера

    public static void main(String[] args) {    // - точка входа
        try {    //Применение конфига к логгеру:
            LogManager.getLogManager().readConfiguration(new FileInputStream(logConfig));
        } catch (Exception e) {   // - обработка возникающих исключений
            System.err.println(logConfig + " is unavailable!");
            return;
        }
        if (args.length != 1) { // - обработка случая неверного количества переданных аргументов
            System.err.println("Wrong number of arguments!");
            return;
        }
        Manager manager = new Manager();    // - создание экземпляра менеджера
        RC code = manager.setConfig(args[0]);   // - установка конфига менеджера
        if (code != RC.CODE_SUCCESS)    // - прекращение работы в случае возникновения ошибки
            return;
        code = manager.setPipeline();   // - создание конвеера
        if (code != RC.CODE_SUCCESS)    // - прекращение работы в случае возникновения ошибки
            return;
        manager.run();  // - запуск конвеера
    }
}
