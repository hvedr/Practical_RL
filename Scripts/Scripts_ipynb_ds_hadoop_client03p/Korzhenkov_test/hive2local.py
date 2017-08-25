# encoding=utf-8


class LinkHiveLocal:
    """
    Класс позволяет связать директорию на linux-машине (виртуальной) и директорию в Hive.
    """

    def __init__(self,
                 hive_dir=None, local_dir='~/'):
        """
        Создает экземпляр класса.

        :param hive_dir:
            Адрес директории в Hive.
            Пример: '/user/hive/warehouse/user_dmkorzhenkov.db/featured_weblog_svmlib/'
        :type hive_dir: string
        :param local_dir:
            Адрес локальной директории linux.
            Пример: '/home/d.m.korzhenkov/liru_segment/'
        :type local_dir: string
        """

        if (hive_dir is not None) and (hive_dir[-1] != '/'):
            hive_dir += '/'
        if local_dir[-1] != '/':
            local_dir += '/'
        self.hive_dir = hive_dir
        self.local_dir = local_dir

    def cp_table_to_local(self, hive_name=None, local_name='hive_table.txt', copy_all=False):
        """
        Копирует таблицу Hive полностью(!) в текстовый файл на локальную машину.
        Возвращает полный путь к  текстовому файлу на локальной машине.

        :param hive_name:
            Имя таблицы (либо файла таблицы) в Hive.
            Если не указано, считается, что на самом деле под видом директории в конструктор класса
            была передана искомая таблица.
            Внимание: таблица должна храниться как textfile!
            Пример: '000000_0' (файл таблицы)
        :type hive_name: string
        :param local_name:
            Имя целевого текстового файла в локальной директории. Пример: 'hivefile'
        :type local_name: string
        :param copy_all:
            Флаг, копировать ли все части таблицы, если она сама состоит из нескольких файлов.
        :type copy_all: bool
        :return: string
        """

        from subprocess import call

        h_dir = self.hive_dir
        if hive_name is None:
            if h_dir[-1] == '/':
                h_dir = h_dir[:-1]
            hive_name = ''
            copy_all = True

        s = 'hadoop fs -text ' + h_dir + hive_name
        if copy_all:
            s += r'/*'
        s += ' > '
        s += self.local_dir + local_name

        call(s, shell=True)

        return self.local_dir + local_name

    def ls_hive(self, add_path=''):
        """
        Возвращает список директорий (файлов) в директории Hive в виде массива.

        :param add_path:
            Дополнительный путь внутри заданной директории Hive. Пример: 'tablename/'
        :type add_path: string
        :return: list[string]
        """

        import re
        from subprocess import check_output

        path = self.hive_dir + add_path
        command = 'hadoop fs -ls ' + path
        output = check_output(command, shell=True)
        ls = re.findall(path + r'(\w+)\n', output)

        return ls

    def paste_local(self, files, result_name='result_paste.txt', sep=','):
        """
        Склеивает построчно несколько текстовых файлов на локальной машине, разделяя заданным символом.
        Возвращает полный путь к файлу с результатом.

        :param files:
            Список имен текстовых файлов из заданной локальной директории.
            Пример: ['file1.txt', 'file2.txt']
        :type files: list[string]
        :param result_name:
            Имя итогового файла. Пример: 'file'
        :type result_name: string
        :param sep:
            Разделитель. Пример: ','
        :type sep: string
        :return: string
        """

        from subprocess import call

        command = "paste -d '" + sep + "' "
        for file in files:
            command += (self.local_dir + file + " ")
        command += " > "
        command += (self.local_dir + result_name)

        call(command, shell=True)

        return self.local_dir + result_name

    def cp_table_to_hive(self, local_name, hive_name=None):
        """
        Копирует локальный текстовый файл в HDFS. Возвращает полный путь к файлу в HDFS.

        :param local_name:
            Имя локального файла в заданной классом директории. Пример: 'filename.txt'
        :type local_name: string
        :param hive_name:
            Имя файла, которое он получит в директории Hive. Если не задано, полагается равным local_name.
            Пример: 'file_hdfs.txt'
        :return: string
        """

        from subprocess import call

        if hive_name is None:
            hive_name = local_name

        command = 'hadoop fs -put ' + self.local_dir + local_name
        command += ' ' + self.hive_dir + hive_name

        call(command, shell=True)

        return self.hive_dir + hive_name
