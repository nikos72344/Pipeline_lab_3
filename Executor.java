import ru.spbstu.pipeline.*;

import java.util.*;
import java.util.logging.Logger;

//Класс, выполнящий циклический сдвиг

public class Executor implements IExecutor {
    private static Logger LOGGER;   // - ссылка логгер

    //Перечисление токенов конфига с вложенным строчным представлением

    private enum Tokens {
        SHIFT_AMOUNT("SHIFT_AMOUNT", 1),
        SHIFT_DIRECTION("SHIFT_DIRECTION", 1),
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
                    res = new Byte[buffer.length];
                    for (int i = 0; i < buffer.length; i++)
                        res[i] = (Byte) buffer[i];
                    break;
                case SHORT:
                    res = new Short[buffer.length];
                    short one;
                    for (int i = 0; i < buffer.length; i++) {
                        one = (short) buffer[i];
                        res[i] = (Short) one;
                    }
                    break;
                case CHAR:
                    res = new Character[buffer.length];
                    char two;
                    for (int i = 0; i < buffer.length; i++) {
                        two = (char) buffer[i];
                        res[i] = (Character) two;
                    }
                    break;
            }
            LOGGER.info("Data converted into \"" + consumerType.toString() + "\" type");
            return res;
        }
    }

    private final static int BYTE_SIZE = 8; // - размер байта в битах

    private IProducer producer; // - ссылка на производителя
    private IConsumer consumer; // - ссылка на потребителя
    private IMediator mediator; // - ссылка на посредника производителя

    private TYPE[] supportedTypes;  // - массив поддерживаемых этим модулем типов данных
    private TYPE producerType;  // - установленный тип данных для приема от производителя
    private TYPE consumerType;  // - установленный тип данных для передачи потребителю

    private String configFileName;  // - имя файла конфига
    private Map<String, Queue<String>> map; // - словарь с содержимым конфига

    //Перечисление допустимых значений токена направления сдвига с их строчным представлением

    private enum ShiftDirValues {
        wLEFT("left"),
        wRIGHT("right"),
        nLEFT("-1"),
        nRIGHT("1");

        private String title;

        ShiftDirValues(String title) {
            this.title = title;
        }
    }

    //Перечисление возможных значений направления сдвига

    private enum ShiftDirection {
        LEFT,
        RIGHT;
    }

    private int shiftAmount;    // - величина сдвига
    private ShiftDirection shiftDirection;  // - направление сдвига
    private byte[] buffer;  // - буфер хранения данных

    //Конструктор

    public Executor(Logger logger) {
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
        if (consumer == null) {
            LOGGER.severe("Wrong consumer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        this.consumer = consumer;
        LOGGER.info("Consumer set successfully");
        return RC.CODE_SUCCESS;
    }

    //Преобразование строчного представления значения токена в соответствующий тип данных

    private RC valueConverting(Tokens t, Queue<String> queue) {
        switch (t) {
            case SHIFT_AMOUNT:
                try {   //Преобразовние строки в целое значение
                    shiftAmount = Integer.parseInt(queue.remove());
                } catch (NumberFormatException e) {
                    LOGGER.severe("Invalid \"" + t.title + "\" value");
                    return RC.CODE_CONFIG_SEMANTIC_ERROR;
                }
                if (shiftAmount < 1) {  // - проверка допустимого диапазона значений
                    LOGGER.severe("Invalid \"" + t.title + "\" value");
                    return RC.CODE_CONFIG_SEMANTIC_ERROR;
                }
                break;
            case SHIFT_DIRECTION:   //Установка направления сдвига
                String value = queue.remove().toLowerCase();
                if (value.equals(ShiftDirValues.wLEFT.title) || value.equals(ShiftDirValues.nLEFT.title))
                    shiftDirection = ShiftDirection.LEFT;
                else if (value.equals(ShiftDirValues.wRIGHT.title) || value.equals(ShiftDirValues.nRIGHT.title))
                    shiftDirection = ShiftDirection.RIGHT;
                else {
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

    //Циклический побитовый сдвиг влево

    private byte shiftLeft(byte data) {
        int x = data & 0xFF;
        int y = shiftAmount % BYTE_SIZE;
        return (byte) ((x << y) | (x >> (BYTE_SIZE - y)));
    }

    //Циклический побитовый сдвиг вправо

    private byte shiftRight(byte data) {
        int x = data & 0xFF;
        int y = shiftAmount % BYTE_SIZE;
        return (byte) ((x >> y) | (x << (BYTE_SIZE - y)));
    }

    //Метод, производящий циклический побитовый сдвиг

    private byte[] doShift() {
        if (buffer == null) { // - обработка случая достижения конца файла
            LOGGER.info("There is no data to shift");
            return null;
        }
        switch (shiftDirection) {
            case LEFT:  //Побитовый сдвиг влево
                for (int i = 0; i < buffer.length; i++)
                    buffer[i] = shiftLeft(buffer[i]);
                break;
            case RIGHT: //Побитовый сдвиг вправо
                for (int i = 0; i < buffer.length; i++)
                    buffer[i] = shiftRight(buffer[i]);
                break;
        }
        LOGGER.info("Data is shifted");
        return buffer;
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

    //Метод, получающий порцию данных у посредника, производящий циклический сдвиг и запускающий своего потребителя

    public RC execute() {
        buffer = convertBuffer(mediator.getData());
        buffer = doShift();
        consumer.execute();
        return RC.CODE_SUCCESS;
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