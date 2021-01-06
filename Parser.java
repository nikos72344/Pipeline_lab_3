import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.RC;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.logging.Logger;

//Класс, производящий чтение и синтаксический парсинг строк

class Parser implements IConfigurable {
    private static Logger LOGGER;   // - ссылка на логгер

    private static String DELIMITER;    // - значение разделителя
    private static String SPACE = " ";  // - значение пробела
    private static String NULL_STRING = ""; // - значение пустой строки

    private String configFileName;  // - имя конфига для парсинга
    private ArrayList<String> rawData;  // - строки, не разделенные на слова, конфига
    private ArrayList<ArrayList<String>> data;  // - разделенные на слова строки конфига

    //Конструктор, устанавливающий соответствующий логгер

    public Parser(Logger logger) {
        LOGGER = logger;
    }

    //Установка имени файла конфига

    public RC setConfig(String configFileName) {
        this.configFileName = configFileName;
        LOGGER.info("Config file name \"" + this.configFileName + "\" is set");
        return RC.CODE_SUCCESS;
    }

    //Установка значения разделителя

    public RC setDelimiter(String delimiter) {
        DELIMITER = delimiter;
        LOGGER.info("Delimiter is set");
        return RC.CODE_SUCCESS;
    }

    //Метод чтения и разбиения на строки файла конфига

    private RC read() {
        rawData = new ArrayList<String>();
        try {
            File file = new File(configFileName);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String str = bufferedReader.readLine();
            while (str != null) {
                rawData.add(str);
                str = bufferedReader.readLine();
            }
            bufferedReader.close();
            fileReader.close();
        } catch (Exception e) {
            LOGGER.severe("An error occurred during reading \"" + configFileName + "\" config file");
            return RC.CODE_CONFIG_GRAMMAR_ERROR;
        }
        LOGGER.info("Config file \"" + configFileName + "\" read successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод, производящий деление строк на слова

    private RC parse() {
        data = new ArrayList<ArrayList<String>>();
        for (String str : rawData) {
            data.add(new ArrayList<String>());
            String[] one = str.split(DELIMITER);
            for (String i : one) {
                String[] two = i.trim().split(SPACE);
                for (String j : two)
                    if (!j.equals(NULL_STRING))
                        data.get(data.size() - 1).add(j);
            }
        }
        LOGGER.info("Config file \"" + configFileName + "\" parsed successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод, запускающий чтение и парсинг конфига

    public RC run() {
        RC code = read();
        if (code != RC.CODE_SUCCESS)
            return code;
        return parse();
    }

    //Метод, возвращающий разделенные на слова строки

    public ArrayList<ArrayList<String>> getStrings() {
        return data;
    }
}
