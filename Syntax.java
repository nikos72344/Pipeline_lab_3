import ru.spbstu.pipeline.BaseGrammar;
import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.RC;

import java.util.*;
import java.util.logging.Logger;

//Класс, вызывающий парсер конфига, а также проводящий синтаксическую проверку содежимого файла

public class Syntax extends BaseGrammar implements IConfigurable {
    private static Logger LOGGER;   // - ссылка на логгер

    private final static int WORDS_NUM = 2; // - значение минимального количества слов в стоке

    private String configFileName;  // - имя конфигурационного файла
    private ArrayList<ArrayList<String>> data;  // - контейнер разделенных на слова строк
    private Map<String, Queue<String>> map; // - словарь, хранящий иформацию: токен - значение

    //Конструктор

    public Syntax(Logger logger, String[] tokens) {
        super(tokens);
        LOGGER = logger;
    }

    //Установка имени файла конфига

    public RC setConfig(String configFileName) {
        this.configFileName = configFileName;
        LOGGER.info("Config file name \"" + this.configFileName + "\" is set");
        return RC.CODE_SUCCESS;
    }

    //Метод, вызывающий парсер конфига

    public RC readConfig() {
        Parser parser = new Parser(LOGGER);
        parser.setConfig(configFileName);
        parser.setDelimiter(delimiter());
        RC code = parser.run();
        if (code != RC.CODE_SUCCESS)
            return code;
        data = parser.getStrings();
        LOGGER.info("Config file \"" + configFileName + "\" read successfully");
        return RC.CODE_SUCCESS;
    }

    //Метод, заполняющий словарь данными

    private RC fillMap(ArrayList<String> arr) {
        if (arr.size() < WORDS_NUM) {
            LOGGER.severe("Wrong amount of \"" + arr.get(0) + "\" values");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        Queue<String> queue = new LinkedList<String>();
        for (int i = 1; i < arr.size(); i++)
            queue.offer(arr.get(i));
        map.put(arr.get(0), queue);
        return RC.CODE_SUCCESS;
    }

    //Метод, производящий семантический анализ содержимого конфигурационного файла

    public RC run() {
        RC code = RC.CODE_SUCCESS;
        if (data.size() != numberTokens()) {
            LOGGER.severe("Wrong amount of tokens in \"" + configFileName + "\" config file");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        map = new HashMap<String, Queue<String>>();
        for (int i = 0; i < numberTokens(); i++)
            for (ArrayList<String> arr : data)
                if (arr.get(0).equals(token(i))) {
                    code = fillMap(arr);
                    if (code != RC.CODE_SUCCESS)
                        return code;
                    break;
                }
        if (map.size() != numberTokens()) {
            LOGGER.severe("Invalid token in \"" + configFileName + "\" config file");
            return RC.CODE_CONFIG_GRAMMAR_ERROR;
        }
        LOGGER.info("\"" + configFileName + "\" config file tokens are valid");
        return RC.CODE_SUCCESS;
    }

    //Метод, возвращающий словарь

    public Map<String, Queue<String>> getMap() {
        return map;
    }
}
