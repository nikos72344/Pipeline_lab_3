import ru.spbstu.pipeline.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.logging.Logger;

//Класс менеджера

public class Manager implements IConfigurable {
    private final static Logger LOGGER = Logger.getLogger(Manager.class.getName()); // - создание логгера

    //Перечисление токенов конфига с вложенным строчным представлением

    private enum Tokens {
        READER_CONFIG("READER_CONFIG"),
        EXECUTOR_CONFIG("EXECUTOR_CONFIG"),
        WRITER_CONFIG("WRITER_CONFIG"),
        INPUT_FILE("INPUT_FILE"),
        OUTPUT_FILE("OUTPUT_FILE"),
        READER_NAME("READER_NAME"),
        EXECUTOR_NAME("EXECUTOR_NAME"),
        WRITER_NAME("WRITER_NAME"),
        ORDER("ORDER");

        private String title;

        Tokens(String title) {
            this.title = title;
        }
    }

    private String configFileName;  // - имя файла конфига

    private Map<String, Queue<String>> map; // - словарь с содержимым конфига
    private Queue<IPipelineStep> queue; // - очередь модулей конвейера

    private IConsumer starter;  // - стартовый модуль

    private FileInputStream fis;    // - поток чтения
    private FileOutputStream fos;   // - поток записи

    //Метод проверки количества значений для токенов

    private RC dataValidation() {
        if (map.get(Tokens.INPUT_FILE.title).size() > 1) {
            LOGGER.severe("Wrong amount of \"" + Tokens.INPUT_FILE.title + "\" values");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        if (map.get(Tokens.OUTPUT_FILE.title).size() > 1) {
            LOGGER.severe("Wrong amount of \"" + Tokens.OUTPUT_FILE.title + "\" values");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        return RC.CODE_SUCCESS;
    }

    //Метод чтения и обработки конфига метода

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

    //Метод обработки текстовых значений модулей и создания очереди подключения модулей

    private RC setQueue() {
        IPipelineStep exec;

        Queue<String> order = map.get(Tokens.ORDER.title);

        queue = new LinkedList<IPipelineStep>();    // - создание очереди

        while (true) {
            try {
                String module = order.remove();
                try {   //Проверка наличия модуля чтения
                    if (module.equals(map.get(Tokens.READER_NAME.title).peek())) {
                        try {   // - создание модуля чтения
                            exec = (IReader) Class.forName(map.get(Tokens.READER_NAME.title).remove()).getDeclaredConstructor(Logger.class).newInstance(LOGGER);
                        } catch (Exception e) {
                            LOGGER.severe("Class not found");
                            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                        }
                        queue.offer(exec);    // - добавление в очередь
                        continue;
                    }
                } catch (NoSuchElementException e) {
                    LOGGER.severe("Wrong amount of " + Tokens.READER_NAME.title + " values");
                    return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                } catch (NullPointerException e) {
                }
                //Проверка наличия модуля обработки
                try {
                    if (module.equals(map.get(Tokens.EXECUTOR_NAME.title).peek())) {
                        try {   // - создание модуля обработки
                            exec = (IExecutor) Class.forName(map.get(Tokens.EXECUTOR_NAME.title).remove()).getDeclaredConstructor(Logger.class).newInstance(LOGGER);
                        } catch (Exception e) {
                            LOGGER.severe("Class not found");
                            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                        }
                        queue.offer(exec);  // - добавление в очередь
                        continue;
                    }
                } catch (NoSuchElementException e) {
                    LOGGER.severe("Wrong amount of " + Tokens.EXECUTOR_NAME.title + " values");
                    return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                } catch (NullPointerException e) {
                }
                //Проверка наличия модуля записи
                try {
                    if (module.equals(map.get(Tokens.WRITER_NAME.title).peek())) {
                        try {   // - создание модуля записи
                            exec = (IWriter) Class.forName(map.get(Tokens.WRITER_NAME.title).remove()).getDeclaredConstructor(Logger.class).newInstance(LOGGER);
                        } catch (Exception e) {
                            LOGGER.severe("Class not found");
                            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                        }
                        queue.offer(exec);    // - добавление в очередь
                        continue;
                    }
                } catch (NoSuchElementException e) {
                    LOGGER.severe("Wrong amount of " + Tokens.WRITER_NAME.title + " values");
                    return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                } catch (NullPointerException e) {
                }
                //Обработка случая нераспознанного модуля конвейера:
                LOGGER.severe("Unrecognized module");
                return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
            } catch (NoSuchElementException e) {
                break;
            }
        }
        LOGGER.info("Queue is set");
        return RC.CODE_SUCCESS;
    }

    //Метод, устанавливающий конфиг для модуля чтения, открывающий и передающий ему поток чтения

    private RC setReader(IReader reader) {
        RC code = RC.CODE_SUCCESS;

        try {
            code = reader.setConfig(map.get(Tokens.READER_CONFIG.title).remove());    // - установка соответствующего конфига модулю чтения
        } catch (NoSuchElementException e) {
            LOGGER.severe("Wrong amount of " + Tokens.READER_CONFIG.title + " values");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        if (code != RC.CODE_SUCCESS)
            return code;

        try {
            fis = new FileInputStream(map.get(Tokens.INPUT_FILE.title).remove());   // - открытие потока чтения
            code = reader.setInputStream(fis);  // - передача его в модуль чтени
        } catch (NoSuchElementException e) {
            LOGGER.severe("Wrong amount of " + Tokens.INPUT_FILE.title + " values");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        } catch (FileNotFoundException e) { // - обработка исключения
            LOGGER.severe("Input stream is invalid");
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        return code;
    }

    //Метод, устанавливающий конфиг модулю обработки

    private RC setExecutor(IExecutor executor) {
        try {
            return executor.setConfig(map.get(Tokens.EXECUTOR_CONFIG.title).remove());    // - установка соответствующего конфига модулю обработки
        } catch (NoSuchElementException e) {
            LOGGER.severe("Wrong amount of " + Tokens.EXECUTOR_CONFIG.title + " values");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
    }

    //Метод, устанавливающий конфиг модулю записи, а также открывающий и передающйи поток для записи

    private RC setWriter(IWriter writer) {
        RC code = RC.CODE_SUCCESS;

        try {
            code = writer.setConfig(map.get(Tokens.WRITER_CONFIG.title).remove());    // - установка соответствующего конфига модулю записи
        } catch (NoSuchElementException e) {
            LOGGER.severe("Wrong amount of " + Tokens.WRITER_CONFIG.title + " values");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        if (code != RC.CODE_SUCCESS)
            return code;

        try {
            fos = new FileOutputStream(map.get(Tokens.OUTPUT_FILE.title).remove()); // - открытие потока записи
            code = writer.setOutputStream(fos); // - передача его в модуль записи
        } catch (NoSuchElementException e) {
            LOGGER.severe("Wrong amount of " + Tokens.OUTPUT_FILE.title + " values");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        } catch (FileNotFoundException e) { // - обработка исключения
            LOGGER.severe("Output stream is invalid");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }
        return code;
    }

    //Установка модулей на конвейер

    private RC setModules() {
        RC code = RC.CODE_SUCCESS;
        IPipelineStep producer = null, module, consumer;
        while (true) {
            try {
                module = queue.remove();    // - извлечение модуля из очереди
                consumer = queue.peek();    // - получение потребителя модуля без извлечения

                if (producer == null)
                    starter = (IConsumer) module; // - установка стартового модуля

                if (module instanceof IReader)
                    code = setReader((IReader) module);    // - настройка модуля чтения
                if (module instanceof IExecutor)
                    code = setExecutor((IExecutor) module); // - настройка модуля обработки
                if (module instanceof IWriter)   // - настройка модуля записи
                    code = setWriter((IWriter) module);
                if (code != RC.CODE_SUCCESS)
                    return code;

                code = module.setProducer((IProducer) producer);  // - установка производителя
                if (code != RC.CODE_SUCCESS)
                    return code;
                code = module.setConsumer((IConsumer) consumer);  // - установка потребителя
                if (code != RC.CODE_SUCCESS)
                    return code;

                producer = module;  // - установка актуального модуля производителем
            } catch (NoSuchElementException e) {  // - обработка случая пустой очереди
                break;
            }
        }
        LOGGER.info("Pipeline set successfully");
        return code;
    }

    //Метод настройки конвеера

    public RC setPipeline() {
        RC code = RC.CODE_SUCCESS;

        code = setQueue(); // - создание очереди
        if (code != RC.CODE_SUCCESS)
            return code;

        code = setModules();    // - "сборка" конвейера
        if (code != RC.CODE_SUCCESS)
            return code;

        return code;
    }

    //Метод, запускающий конвеер, а также закрывающий потоки чтения/записи

    public RC run() {
        RC code = RC.CODE_SUCCESS;

        code = starter.execute();    // - запуск конвейера

        try {
            fis.close();    // - закрытие потока чтения
        } catch (IOException e) {   // - обработка исключения
            LOGGER.severe("Input stream is invalid");
            code = RC.CODE_INVALID_INPUT_STREAM;
        }
        try {
            fos.close();    // - закртыие потока записи
        } catch (IOException e) {   // - обработка исключения
            LOGGER.severe("Output stream is invalid");
            code = RC.CODE_INVALID_OUTPUT_STREAM;
        }
        return code;
    }
}