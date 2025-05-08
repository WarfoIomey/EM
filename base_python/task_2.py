import random


class Cell:

    def __init__(
            self,
            mine: bool = False,
            around_mines: int = 0,
            fl_open: bool = False
    ):
        self.around_mines: int = around_mines
        self.mine: bool = mine
        self.fl_open: bool = fl_open

    def open(self) -> None:
        self.fl_open = True

    def __str__(self) -> str:
        if not self.fl_open:
            return '#'
        else:
            return '*' if self.mine else str(self.around_mines)


class GamePole:

    def __init__(self, N: int, M: int):
        self.cells: int = N
        self.mines: int = M
        self.create_pole()
        self.mine_distribution()
        self.game_over: bool = False
        self.win: bool = False

    def create_pole(self) -> None:
        """Создание пустого поля."""
        self.pole: list[list] = [
            [Cell() for _ in range(self.cells)] for _ in range(self.cells)
        ]

    def counting_mines(self, row: int, col: int) -> None:
        """Обновление счетчиков мин у соседних клеток."""
        for row_index in range(max(0, row - 1), min(self.cells, row + 2)):
            for col_index in range(max(0, col - 1), min(self.cells, col + 2)):
                if row_index == row and col_index == col:
                    continue
                self.pole[row_index][col_index].around_mines += 1

    def mine_distribution(self) -> None:
        """Распределение мин по игровому полю."""
        mines_placed: int = 0
        while mines_placed < self.mines:
            row: int = random.randint(0, self.cells - 1)
            col: int = random.randint(0, self.cells - 1)
            if not self.pole[row][col].mine:
                self.pole[row][col].mine = True
                self.counting_mines(row, col)
                mines_placed += 1

    def show(self) -> str:
        """Отображение текущего состояния поля."""
        result: str = ''
        for string in self.pole:
            for column in string:
                result += str(column) + ' '
            result += '\n'
        return result

    def reveal_all(self) -> None:
        """Открытие всех клеток (при завершении игры)."""
        for row in self.pole:
            for cell in row:
                cell.open()

    def open_cell(self, row: int, col: int) -> None:
        """Открытие клетки игрового поля."""
        if self.pole[row][col].fl_open or self.game_over:
            return
        cell = self.pole[row][col]
        cell.open()
        if cell.mine:
            self.game_over = True
            return
        if cell.around_mines == 0:
            for row_index in range(max(0, row - 1), min(self.cells, row + 2)):
                for col_index in range(
                    max(0, col - 1),
                    min(self.cells, col + 2)
                ):
                    self.open_cell(row_index, col_index)

    def check_win(self) -> bool:
        """Проверка условий победы."""
        if self.game_over:
            return False
        for row in self.pole:
            for cell in row:
                if not cell.mine and not cell.fl_open:
                    return False
        self.win = True
        return True


def main():
    size = 10
    mines = 12
    game = GamePole(size, mines)
    while True:
        print("\nТекущее поле:")
        print(game.show())
        if game.game_over:
            game.reveal_all()
            print("\nИгра окончена! Вы проиграли!")
            print("Финальное поле:")
            print(game.show())
            break
        if game.check_win():
            game.reveal_all()
            print("\nПоздравляем! Вы выиграли!")
            print("Финальное поле:")
            print(game.show())
            break
        try:
            coords = input(
                "Введите строку и столбец (0-9) через пробел: "
            ).split()
            if len(coords) != 2:
                raise ValueError
            row, col = map(int, coords)
            if not (0 <= row < size and 0 <= col < size):
                print("Ошибка: координаты должны быть от 0 до 9!")
                continue
            game.open_cell(row, col)
        except ValueError:
            print("Ошибка: введите два числа через пробел!")


if __name__ == '__main__':
    # main()
