from src.creds import token
from src.bot import PokestarBot


def main():
    bot = PokestarBot()
    bot.run(token)


if __name__ == "__main__":
    main()
