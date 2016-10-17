/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbfbytereadertest;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author spidchenko.d
 */

class Field{
    String name;    //Название столбца
    int size;       //Ширина столбца в байтах
    int offset;     //Смещение от начала записи в байтах
        
    public Field(String newName, int newSize, int newOffset){
        name = newName;
        size = newSize;
        offset = newOffset;
    }
        
    static Field[] initializeFieldsArray(int fieldsCount){
        Field [] fieldArrayReturn = new Field [fieldsCount];  //Массив столбцов
        
        for (int i = 0; i< fieldArrayReturn.length; i++){
                fieldArrayReturn[i] = new Field("", 0, 0);
            }
        return fieldArrayReturn;
    }
}

class DbfFile {
    static final int HEADER_FIELD_LENGTH = 32;  //Длина описания столбца - 32 байта
    static final int CURRENT_FIELD_NAME = 9;    //0-9 байты
    static final int CURRENT_FIELD_LENGTH = 16; //16й байт с длиной текущего столбца
    static final Charset FILE_CHARSET = Charset.forName("cp866");//Кодировка
    private int endOfHeaderOffset = 0;           //Смещение в байтах конца заголовка
    private int numOfFields = 0;                 //Количество столбцов таблицы
    private int numOfRecords = 0;                //Количество записей в таблице
    private int oneRecordLength = 0;             //Длина одной записи в байтах
    private Field [] fieldArray;
        //------
    private String filePath = "";//E:\\0302.dbf"; 
        
    public DbfFile(String filePathToOpen){
        filePath = filePathToOpen;
        //--
        FileInputStream inputStream = null;
        byte[] byteBufferArray = new byte[1024];   //Байтовый буфер для чтения. Самая длинная запись которую я видел - 505 байт, пусть будет в 2 раза больше
                
        try{
            inputStream = new FileInputStream(filePath);
            inputStream.read(byteBufferArray, 0, 32);
            //Делаем unsigned byte массив [4-7] байтов (количество записей, старший байт справа)
            int [] byteArray = new int[4];
            for(int i = 0; i<4; i++){
                byteArray[i] = (byteBufferArray[i+4]>0)?byteBufferArray[i+4]:(byteBufferArray[i+4] & 0xFF);
            }
            //сдвигаем байты (старший слева) и получаем количество записей (32бит число)
            numOfRecords = byteArray[0]|(byteArray[1]<<8)|(byteArray[2]<<16)|(byteArray[3]<<24);
            //Делаем unsigned byte массив [10-11] байтов (длина одной записи, старший байт справа)
            for(int i = 0; i<2; i++){
                byteArray[i] = (byteBufferArray[i+10]>0)?byteBufferArray[i+10]:(byteBufferArray[i+10] & 0xFF);
            }           
            //сдвигаем байты (старший слева) и получаем длину одной записи (16бит число)
            oneRecordLength = byteArray[0]|(byteArray[1]<<8);
                                
            while(inputStream.read()!=0xD){     //Поиск конца заголовка: байт 0xD
                endOfHeaderOffset++;
            }
        
            //Считаем количество столбцов в таблице
            numOfFields = endOfHeaderOffset/HEADER_FIELD_LENGTH; 
        
            inputStream = new FileInputStream(filePath);  //Откроем еше раз, чтобы вернуться в начало файла, как иначе хз
            inputStream.skip(32);       //Пропустили заголовок

            //Парсим описания столбцов таблицы:
            fieldArray = Field.initializeFieldsArray(numOfFields);  //Инициализируем массив столбцов
            
            for (int i = 0; i < fieldArray.length; i++){
                inputStream.read(byteBufferArray, 0, HEADER_FIELD_LENGTH);    //32 байта 
                
                //Название столбца (вытащили из байтового массива и убрали пробелы с конца одной коммандой! >:3 )
                //new String корректно отработает с default charset ASCII, на линуксе или в Японии с UTF Default будут проблемы 
                fieldArray[i].name = new String(Arrays.copyOfRange(byteBufferArray, 0, CURRENT_FIELD_NAME)).trim();  //9 байт
                
                //Размер столбца
                fieldArray[i].size = (byteBufferArray[CURRENT_FIELD_LENGTH]>0)?
                    byteBufferArray[CURRENT_FIELD_LENGTH]:
                    byteBufferArray[CURRENT_FIELD_LENGTH] & 0xFF;
                //Сдвиг от начала записи в байтах
                if (i != 0){
                    fieldArray[i].offset = fieldArray[i-1].offset+fieldArray[i-1].size;
                } else{
                    fieldArray[i].offset = 0;
                }
                
                //Сдвиг от начала записи в байтах
            }
            
            System.out.print("\n");
            System.out.format("endOfHeaderOffset:%4d \nnumOfFields:%3d \nnumOfRecords:%4d \noneRecordLength:%4d\n\n", endOfHeaderOffset, numOfFields, numOfRecords, oneRecordLength);
            
            for (Field fieldArray1 : fieldArray) {
                System.out.format("%10s | %5d | %5d \n", fieldArray1.name, fieldArray1.size, fieldArray1.offset);
            }
            System.out.print("\n");
            
            //Файловый курсор сейчас перед 0xD, пропустим [0xD, 0x0]
            inputStream.skip(2);
            for (int i = 0; i < numOfRecords; i++){
                //Считали одну запись в byteBufferArray
                inputStream.read(byteBufferArray, 0, oneRecordLength);
                //Декодировали массив byteBufferArray, обернутый в байтбуффер в UTF-16 
                //Имеем на выходе строку UTF-16 с полями из DBF файла
                System.out.println(FILE_CHARSET.decode(ByteBuffer.wrap(byteBufferArray,0, oneRecordLength)));                
            }
            
        } catch (IOException ex) {
            Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (inputStream != null) {try {
                inputStream.close();
                } catch (IOException ex) {
                    Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
            //--
    }//End of DbfFile Constructor method
}

public class DbfByteReaderTest {

    /**
     * @param args the command line arguments
     */
    


    public static void main(String[] args) {
        DbfFile currentDbf = new DbfFile("E:\\0302.dbf");
    }
}
