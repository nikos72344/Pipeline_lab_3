import ru.spbstu.pipeline.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

//Класс буфера для записи

class Buffer {
    private byte[] buffer;  // - буфер
    private int filled; // - количество занятых ячеек
    FileOutputStream fos;   // - поток для записи

    //Метод установки потока для записи

    public RC setOutputStream(FileOutputStream fos) {
        this.fos = fos;
        return RC.CODE_SUCCESS;
    }

    //Метод установки буфера определенного размера

    public RC setBuffer(int size) {
        buffer = new byte[size];
        return RC.CODE_SUCCESS;
    }

    //Провера на заполненность

    public boolean isFull() {
        return filled == buffer.length;
    }

    //Добавление в буффер еще одного значения

    public boolean add(byte val) {
        if (!isFull()) { // - проверка на заполненность
            buffer[filled] = val;   // - добавление при наличии свободных ячеек
            filled++;   // - увеличения счетчика занятых ячеек
        } else
            return false;   // - возврат при неуспешном добавлении
        return true;    // - возврат при успешном добавлении
    }

    //Запись буффера в файл

    public RC write() {
        try {
            fos.write(buffer, 0, filled);   // - запись
            filled = 0;   // - обнуление счетчика занятых ячеек
        } catch (IOException e) { // - обработка исключения
            return RC.CODE_FAILED_TO_WRITE;
        }
        return RC.CODE_SUCCESS;
    }
}

//Класс модуля записи данных в файл

public class Writer implements IWriter {
    private static Logger LOGGER;   // - ссылка на логгер

    //Перечисление токенов конфига с вложенным строчным представлением

    private enum Tokens {
        SIZE_TO_WRITE("SIZE_TO_WRITE", 1),
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

    private IProducer producer; // - ссылка на производителя
    private IMediator mediator; // - ссылка на посредника производителя

    private TYPE[] supportedTypes;  // - массив поддерживаемых этим модулем типов данных
    private TYPE producerType;  // - установленный тип данных для приема от производителя

    private FileOutputStream fos;   // - поток записи

    private String configFileName;  // - имя файла конфига
    private Map<String, Queue<String>> map; // - словарь с содержимым конфига

    private int sizeToWrite;    // - размер буфера для записи
    private Buffer buffer;  // - буфер для записи

    //Конструктор

    public Writer(Logger logger) {
        LOGGER = logger;
    }

    //Нахождение пересечения множеств типов производителя и потребителя

    private TYPE setType(TYPE[] types) {
        TYPE res = null;
        for (int i = 0; i < producer.getOutputTypes().length; i++)
            for (int j = 0; j < supportedTypes.length; j++)
                if (supportedTypes[j].equals(producer.getOutputTypes()[i])) {
                    res = supportedTypes[j];
                    break;
                }
        return res;
    }

    //Установка производителя

    public RC setProducer(IProducer producer) {
        if (producer == null) {
            LOGGER.severe("Wrong producer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        this.producer = producer;
        producerType = setType(producer.getOutputTypes());
        if (producerType == null) {
            LOGGER.severe("No intersecting types");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        mediator = producer.getMediator(producerType);
        LOGGER.info("Producer and Mediator are set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потребителя

    public RC setConsumer(IConsumer consumer) {
        if (consumer != null) {
            LOGGER.severe("Wrong producer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        LOGGER.info("Producer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Установка потока записи

    public RC setOutputStream(FileOutputStream fos) {
        this.fos = fos;
        RC code = buffer.setOutputStream(fos);
        LOGGER.info("Output stream is set");
        return code;
    }

    //Преобразование строчного представления значения токена в соответствующий тип данных

    private RC valueConverting(Tokens t, Queue<String> queue) {
        switch (t) {
            case SIZE_TO_WRITE:
                try {
                    sizeToWrite = Integer.parseInt(queue.remove());
                } catch (NumberFormatException e) {
                    LOGGER.severe("Invalid \"" + t.title + "\" value");
                    return RC.CODE_CONFIG_SEMANTIC_ERROR;
                }
                if (sizeToWrite < 1) {
                    LOGGER.severe("Invalid \"" + t.title + "\" value");
                    return RC.CODE_CONFIG_SEMANTIC_ERROR;
                }
                break;
            case TYPE:
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
        buffer = new Buffer();
        code = buffer.setBuffer(sizeToWrite);
        if (code != RC.CODE_SUCCESS)
            return code;
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

    //Метод приведения переданных данных к установленному типу

    private byte[] convertBuffer(Object data) {
        if (data == null)
            return null;
        byte[] res = null;
        switch (producerType) {
            case BYTE:
                Byte[] bTemp = (Byte[]) data;
                res = new byte[bTemp.length];
                for (int i = 0; i < bTemp.length; i++)
                    res[i] = bTemp[i].byteValue();
                break;
            case SHORT:
                Short[] sTemp = (Short[]) data;
                res = new byte[sTemp.length];
                for (int i = 0; i < sTemp.length; i++)
                    res[i] = sTemp[i].byteValue();
                break;
            case CHAR:
                Character[] cTemp = (Character[]) data;
                res = new byte[cTemp.length];
                for (int i = 0; i < cTemp.length; i++)
                    res[i] = (byte) cTemp[i].charValue();
                break;
        }
        return res;
    }

    //Метод, выполняющий заполнение буфера и своевременную запись его содержимого в файл

    public RC execute() {
        byte[] data = convertBuffer(mediator.getData());    // - получение порции данных
        RC code = RC.CODE_SUCCESS;
        if (data == null) {    // - обработка случая достижения конца файла
            LOGGER.info("Writing the remaining data");
            return buffer.write();  // - запись оставшихся данных в файл
        }
        LOGGER.info("Saving data into buffer");
        for (int i = 0; i < data.length; i++) {
            if (!buffer.add(data[i])) {  // - попытка добавления байта в буфер
                code = buffer.write();    // - при невозможности запись содержимого буфера в файл
                if (code != RC.CODE_SUCCESS) {
                    LOGGER.severe("Couldn't write data to output file");
                    return code;
                }
                LOGGER.info("Writing data from the buffer");
                LOGGER.info("Saving data into buffer");
                buffer.add(data[i]);    // - добавление байта в пустой буфер
            }
        }
        return code;
    }
}