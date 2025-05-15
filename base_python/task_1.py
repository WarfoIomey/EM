class ObjList:

    def __init__(self, data, next_item=None, prev_item=None):
        self.__data = data
        self.__next_item = next_item
        self.__prev_item = prev_item

    def set_next(self, obj) -> None:
        self.__next_item = obj

    def set_prev(self, obj) -> None:
        self.__prev_item = obj

    @property
    def next(self):
        return self.__next_item

    @property
    def prev(self):
        return self.__prev_item

    def set_data(self, data) -> None:
        self.__data = data

    @property
    def data(self):
        return self.__data


class LinkedList:

    def __init__(self):
        self.head = None
        self.tail = None

    def add_obj(self, obj) -> None:
        """Добавление нового объекта в конец связного списка."""
        if self.tail is not None:
            self.tail.set_next(obj)
            obj.set_prev(self.tail)
            self.tail = obj
        else:
            self.head = self.tail = obj

    def remove_obj(self) -> None:
        """Удаление последнего объекта из связного списка."""
        current_obj = self.tail
        prev_obj = current_obj.prev
        prev_obj.set_next(current_obj.next)

    def get_data(self) -> list:
        """Получение списка из строк со всех объектов связного списка."""
        current_obj = self.head
        result: list = []
        while current_obj is not None:
            result.append(current_obj.data)
            current_obj = current_obj.next
        return result
