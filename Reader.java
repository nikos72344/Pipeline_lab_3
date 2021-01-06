import ru.spbstu.pipeline.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

//Класс модуля чтения данных

public class Reader implements IReader {
    private static Logger LOGGER;   // - ссылка на логгер

    //Перечисление токенов конфига с вложенным строчным представлением

    private enum Tokens {
        SIZE_TO_READ("SIZE_TO_READ", 1),
        TYPE("TYPE", 3);

        private String title;
        private int valNum;

        Tokens(String title, int valNum) {
            this.title = title;
            this.valNum = valNum;
        }

        //Метод проверки количества значений для токенов

        public boolean isValNumValid(Queue<String> queue) {
            if (queue.size() > valNum)
                return false;
            return true;
        }
    }

    //Внутренний класс посредника

    public class Mediator implements IMediator {
        public Object getData() {
            if (buffer == null)
                return null;
            Object[] res = null;
            switch (consumerType) {
                case BYTE:
                    res = new Byte[sizeToRead];
                    for (int i = 0; i < sizeToRead; i++)
                        res[i] = (Byte) buffer[i];
                    break;
                case SHORT:
                    res = new Short[sizeToRead];
                    short one;
                    for (int i = 0; i < sizeToRead; i++) {
                        one = (short) buffer[i];
                        res[i] = (Short) one;
                    }
                    break;
                case CHAR:
                    res = new Character[sizeToRead];
                    char two;
                    for (int i = 0; i < sizeToRead; i++) {
                        two = (char) buffer[i];
                        res[i] = (Character) two;
                    }
                    break;
            }
            LOGGER.info("Data converted into \"" + consumerType.toString() + "\" type");
            return res;
        }
    }

    private IConsumer consumer; // - ссылка на потребителя

    private TYPE[] supportedTypes;  // - массив поддерживаемых этим модулем типов данных
    private TYPE consumerType;  // - установленный тип данных для передачи потребителю

    private FileInputStream fis;    // - поток чтения

    private String configFileName;  // - имя файла конфига
    private Map<String, Queue<String>> map; // - словарь с содержимым конфига

    private int sizeToRead; // - размер порции чтения
    private byte[] buffer;  // - буфер для хранения прочитанных данных

    //Конструктор

    public Reader(Logger logger) {
        LOGGER = logger;
    }

    //Установка производителя

    public RC setProducer(IProducer producer) {
        if (producer != null) {
            LOGGER.severe("Wrong producer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        LOGGER.info("Producer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потребителя

    public RC setConsumer(IConsumer consumer) {
        if (consumer == null) {
            LOGGER.severe("Wrong consumer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        this.consumer = consumer;
        LOGGER.info("Consumer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потока чтения

    public RC setInputStream(FileInputStream fis) {
        this.fis = fis;
        LOGGER.info("Input stream set successfully");
        return RC.CODE_SUCCESS;
    }

    //Преобразование строчного представления значения токена в соответствующий тип данных

    private RC valueConverting(Tokens t, Queue<String> queue) {
        switch (t) {
            case SIZE_TO_READ:
                try {   //Преобразовние строки в целое значение
                    sizeToRead = Integer.parseInt(queue.remove());
                } catch (NumberFormatException e) {
                    LOGGER.severe("Invalid \"" + t.title + "\" value");
                    return RC.CODE_CONFIG_SEMANTIC_ERROR;
                }
                if (sizeToRead < 1) {   // - проверка допустимого диапазона значений
                    LOGGER.severe("Invalid \"" + t.title + "\" value");
                    return RC.CODE_CONFIG_SEMANTIC_ERROR;
                }
                break;
            case TYPE:  //Обработка поддерживаемых модулем типов данных
                supportedTypes = new TYPE[queue.size()];
                try {
                    int i = 0;
                    while (!queue.isEmpty()) {
                        supportedTypes[i] = TYPE.valueOf(queue.remove());
                        i++;
                    }
                } catch (IllegalArgumentException e) {
                    LOGGER.severe("Invalid \"" + t.title + "\" value");
                    return RC.CODE_CONFIG_SEMANTIC_ERROR;
                }
                break;
        }
        return RC.CODE_SUCCESS;
    }

    //Метод, проверящий и обрабатывающий значения токенов в конфиге

    private RC dataValidation() {
        for (Tokens t : Tokens.values()) {
            Queue queue = map.get(t.title);
            if (!t.isValNumValid(queue)) {
                LOGGER.severe("Wrong amount of \"" + t.title + "\" values");
                return RC.CODE_CONFIG_SEMANTIC_ERROR;
            }
            RC code = valueConverting(t, queue);
            if (code != RC.CODE_SUCCESS)
                return code;
        }
        LOGGER.info("\"" + configFileName + "\" config file values are valid");
        return RC.CODE_SUCCESS;
    }

    //Метод чтения и обработки конфига модуля чтения

    private RC readConfig() {
        String[] temp = new String[Tokens.values().length]; // - создание массива текстовых представлений токенов для разбора конфига
        for (int i = 0; i < Tokens.values().length; i++)
            temp[i] = Tokens.values()[i].title;
        Syntax syntax = new Syntax(LOGGER, temp);   // - создание экземпляра класса синтаксической обработки
        syntax.setConfig(configFileName);   // - чтение и парсинг конфига
        RC code = syntax.readConfig();
        if (code != RC.CODE_SUCCESS)
            return code;
        code = syntax.run();    // - проведение синтаксического анализа
        if (code != RC.CODE_SUCCESS)
            return code;
        map = syntax.getMap();  // - получение обработанного содержимого конфига
        code = dataValidation();
        if (code == RC.CODE_SUCCESS)
            LOGGER.info("\"" + configFileName + "\" config file read successfully");
        return code;
    }

    //Метод установки имени конфига, а также работы с ним

    public RC setConfig(String configFileName) {
        if (configFileName == null) {
            LOGGER.severe("Null pointer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        this.configFileName = configFileName;
        LOGGER.info("\"" + this.configFileName + "\" config file name is set");
        return readConfig();
    }

    //Метод выполненяющий чтения данных, а также запуск модуля потребителя

    public RC execute() {
        RC code = RC.CODE_SUCCESS;
        int flag = 0;
        while (true) {
            buffer = new byte[sizeToRead];   // - буфер для порции байтов
            try {
                flag = fis.read(buffer, 0, sizeToRead);   // - чтение
            } catch (IOException e) {   // - обработка исключения
                LOGGER.severe("Couldn't read data from input file");
                return RC.CODE_FAILED_TO_READ;
            }
            /*if (flag < sizeToRead && flag != -1) {    // - обработка случая чтения неполной порции байтов
                LOGGER.severe("Incomplete data");
                return RC.CODE_FAILED_TO_READ;
            }*/
            if (flag == -1) {    // - обработка случая достижения конца файла
                LOGGER.info("All the data was read successfully");
                buffer = null;
                return consumer.execute();
            }
            LOGGER.info("Portion data was read successfully");
            code = consumer.execute();    // - запуск модуля потребителя
            if (code != RC.CODE_SUCCESS)
                return code;
        }
    }

    //Метод, возрващающий потребителю поддерживаемые модулем типы данных

    public TYPE[] getOutputTypes() {
        return supportedTypes;
    }

    //Возвращение экземпляра посредника

    public IMediator getMediator(TYPE type) {
        consumerType = type;
        return new Mediator();
    }
}