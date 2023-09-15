import time
import random
import pygame
from pygame.locals import *
from pygame.sprite import RenderUpdates


SIZE = 40
BACKGROUND_COLOR = (110, 110, 5)


class Apple:
    def __init__(self, parent_screen) -> None:
        self.apple = pygame.image.load("resources/apple.jpg").convert()
        self.parent_screen = parent_screen
        self.x = SIZE * 3
        self.y = SIZE * 3

    def draw(self):
        self.parent_screen.blit(self.apple, (self.x, self.y))
        pygame.display.flip()

    def move(self):
        self.x = random.randint(0, 24) * SIZE
        self.y = random.randint(0, 19) * SIZE


class Snake:
    def __init__(self, parent_screen, length) -> None:
        self.length = length
        self.parent_screen = parent_screen
        self.block = pygame.image.load("resources/block.jpg").convert()
        self.x = [SIZE]*length
        self.y = [SIZE]*length
        self.direction = "down"

    def increase_length(self):
        self.length += 1
        self.x.append(-1)
        self.y.append(-1)

    def draw(self):
        for i in range(self.length):
            self.parent_screen.blit(self.block, (self.x[i], self.y[i]))
        pygame.display.flip()

    def walk(self):

        for i in range(self.length-1, 0, -1):
            self.x[i] = self.x[i-1]
            self.y[i] = self.y[i-1]

        if self.direction == "up":
            self.y[0] -= SIZE
        if self.direction == "down":
            self.y[0] += SIZE
        if self.direction == "left":
            self.x[0] -= SIZE
        if self.direction == "right":
            self.x[0] += SIZE

        self.draw()

    def move_left(self):
        self.direction = "left"
        self.draw()

    def move_right(self):
        self.direction = "right"
        self.draw()

    def move_up(self):
        self.direction = "up"
        self.draw()

    def move_down(self):
        self.direction = "down"
        self.draw()


class Game:
    def __init__(self) -> None:
        pygame.init()
        self.surface = pygame.display.set_mode(size=(1000, 800))

        pygame.mixer.init()
        self.play_bacground_music()

        self.render_background()
        self.snake = Snake(self.surface, 7)
        self.snake.draw()
        self.apple = Apple(self.surface)
        self.apple.draw()

    def is_collision(self, x1, y1, x2, y2):
        if x1 >= x2 and x1 < x2 + SIZE:
            if y1 >= y2 and y1 < y2 + SIZE:
                return True

        return False

    def is_beyond_screen(self, x1, y1, x_min, x_max, y_min, y_max):
        if x1 < x_min or x1 > x_max or y1 < y_min or y1 > y_max:
            return True
        return False

    def render_background(self):
        bg = pygame.image.load("resources/background.jpg")
        self.surface.blit(bg, (0, 0))

    def play_sound(self, sound):
        sound = pygame.mixer.Sound(sound)
        pygame.mixer.Sound.play(sound)

    def play_bacground_music(self):
        pygame.mixer.music.load("resources/bg_music_1.mp3")
        pygame.mixer.music.play()

    def play(self):
        self.render_background()
        self.snake.walk()
        self.apple.draw()
        self.display_score()
        pygame.display.flip()

        # logic of snake colliding with apple
        if self.is_collision(self.snake.x[0], self.snake.y[0], self.apple.x, self.apple.y):
            self.play_sound("resources/1_snake_game_resources_ding.mp3")
            self.snake.increase_length()
            self.apple.move()

        # logic of snake colliding with itself
        for i in range(3, self.snake.length):
            if self.is_collision(self.snake.x[0], self.snake.y[0], self.snake.x[i], self.snake.y[i]):
                self.play_sound("resources/1_snake_game_resources_crash.mp3")
                raise "GAME OVER"

        # logic of snake if colliding with boundry
        if self.is_beyond_screen(self.snake.x[0], self.snake.y[0], x_min=0, y_min=0, x_max=1000, y_max=800):
            self.play_sound(
                "resources/1_snake_game_resources_crash.mp3")
            raise "GAME OVER"

    def display_score(self):
        font = pygame.font.SysFont('arial', 30)
        score = font.render(
            f"Score: {self.snake.length}", True, (255, 255, 255))
        self.surface.blit(score, (800, 10))

    def show_game_over(self):
        self.render_background()
        font = pygame.font.SysFont('arial', 30)
        line1 = font.render(
            f"Score: {self.snake.length}", True, (255, 255, 255)
        )
        self.surface.blit(line1, (200, 300))
        line2 = font.render(
            f"To play again press Enter. To exit press Escape!", True, (
                255, 255, 255)
        )
        self.surface.blit(line2, (200, 350))
        pygame.display.flip()

        pygame.mixer.music.pause()

    def reset(self):
        self.snake = Snake(self.surface, 1)
        self.apple = Apple(self.surface)

    def run(self):
        game_over = True
        pause = False

        while game_over:
            for event in pygame.event.get():
                if event.type == KEYDOWN:
                    if event.key == K_ESCAPE:
                        game_over == False

                    if event.key == K_RETURN:
                        pygame.mixer.music.unpause()
                        pause = False

                    if not pause:
                        if event.key == K_UP:
                            self.snake.move_up()

                        if event.key == K_DOWN:
                            self.snake.move_down()

                        if event.key == K_LEFT:
                            self.snake.move_left()

                        if event.key == K_RIGHT:
                            self.snake.move_right()

                elif event.type == QUIT:
                    game_over = False

            try:
                if not pause:
                    self.play()
            except Exception as e:
                self.show_game_over()
                pause = True
                self.reset()

            time.sleep(0.1)


if __name__ == "__main__":
    game = Game()
    game.run()
