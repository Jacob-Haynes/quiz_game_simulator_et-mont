# Databricks notebook source
def play_game(i):
  import random
  from dataclasses import dataclass, asdict
  from typing import Tuple
  from IPython.display import display

  import numpy as np
  import pandas as pd
  

  @dataclass
  class RarityPoints:
      #asuming a current score of 1-0, format is normalised decimal odds, implied probability as a percentage)
      one_nill: Tuple[int,int] = (3.2, 31.3)
      two_nill: Tuple[int,int] = (4.75, 21.1)
      two_one: Tuple[int,int] = (7, 14.3)
      three_nill: Tuple[int,int] = (13, 7.7)
      three_one: Tuple[int,int] = (19, 5.3)
      three_two: Tuple[int,int] = (51, 2)
      four_nill: Tuple[int,int] = (51, 2)
      one_one: Tuple[int,int] = (4.6, 21.7)
      two_two: Tuple[int,int] = (19, 5.3)
      three_three: Tuple[int,int] = (151, 0.7)
      one_two: Tuple[int,int] = (13, 7.7)
      one_three: Tuple[int,int] = (56, 1.8) 
      two_three: Tuple[int,int] = (71, 1.4)
      one_four: Tuple[int,int] = (201, 0.5)
      two_four: Tuple[int,int] = (201, 0.5)
      other: Tuple[int,int] = (1001, 0.1)


  @dataclass
  class GameConfig:
      num_agents: int = 200_000
      questions_per_round: int = 6
      num_choice_per_question: int = 4
      num_points_per_correct: int = 10_000
      num_points_per_speed: Tuple[int, int] = (1660, 100)
      player_answer_speeds: Tuple[int, int] = (0, 110)
      speed_thresholds: Tuple[int, int] = (0, 100)
      rarity_points: RarityPoints = RarityPoints()
      skill_beta_params: Tuple[int, int] = (8, 2)
      speed_beta_params: Tuple[int, int] = (2, 5)
      match_result: str = 'other'


  @dataclass
  class Player:
      player_id: int
      game_config: GameConfig
      player_skill: int
      points: int = 0
      player_prediction: str = 'other'

      def question_choice(self) -> int:
          return random.choices(
              range(self.game_config.num_choice_per_question),
              weights=[
                  self.player_skill,
                  (1 - self.player_skill) / 3,
                  (1 - self.player_skill) / 3,
                  (1 - self.player_skill) / 3,
              ],
              k=1,
          )[0]
          
      def predict_result(self) -> None:
          odds = vars(self.game_config.rarity_points)
          probs = [percent[1] for percent in odds.values()]
          self.player_prediction = list(odds)[random.choices(range(len(odds.keys())), weights = [prob * self.player_skill for prob in probs], k=1)[0]]
          if self.player_prediction == self.game_config.match_result:
              multiplier = 1 + (1/getattr(self.game_config.rarity_points, self.game_config.match_result)[1])
              self.points = self.points * multiplier

      def question_speed(self) -> int:
          return np.rint(np.random.beta(*self.game_config.speed_beta_params) * self.game_config.speed_thresholds[1]) 
#           return random.randint(self.game_config.player_answer_speeds[0], self.game_config.player_answer_speeds[1])

      def get_speed_points(self, speed: int) -> int:
          if speed <= self.game_config.speed_thresholds[0]:
              return self.game_config.num_points_per_speed[0]
          elif speed > self.game_config.speed_thresholds[1]:
              return 0
          else:
              speed_range = self.game_config.speed_thresholds[1] - self.game_config.speed_thresholds[0]
              frac_speed = (speed - self.game_config.speed_thresholds[1]) / speed_range

              point_range = self.game_config.num_points_per_speed[1] - self.game_config.num_points_per_speed[0]
              frac_points = point_range * frac_speed
              points = frac_points + self.game_config.num_points_per_speed[0]
              return int(points)

      def answer_question(self, correct_answer: int) -> None:
          choice = self.question_choice()
          if choice == correct_answer:
              speed = self.question_speed()
              if speed > self.game_config.speed_thresholds[1]:
                  return
              self.points += self.game_config.num_points_per_correct
              self.points += self.get_speed_points(speed)

  class Game:
      def __init__(self, game_config: GameConfig) -> None:
          self.game_config = game_config
          odds = vars(self.game_config.rarity_points)
          self.game_config.match_result = list(odds)[random.choices(range(len(odds.keys())), weights = [percent[1] for percent in odds.values()], k=1)[0]]
          self.dupe_winners = False
          skill_levels = np.random.beta(*game_config.skill_beta_params, size=game_config.num_agents)
          self.players = [
              Player(player_id, game_config=game_config, player_skill=skill_levels[player_id])
              for player_id in range(game_config.num_agents)
          ]

      def play(self):
          for round_num in range(self.game_config.questions_per_round):
              correct_answer = 0
              for player in self.players:
                  player.answer_question(correct_answer)
          for player in self.players:
              player.predict_result()
          leaderboard = self.leaderboard
          if leaderboard.iloc[0,0] == leaderboard.iloc[1,0]:
              self.dupe_winners = True

      @property
      def leaderboard(self):
          data = [(player.player_id, player.points, player.player_skill, player.player_prediction) for player in self.players]
          leaderboard = pd.DataFrame(data)
          leaderboard.columns = ['player_id', 'points', 'skill', 'prediction']
          return leaderboard.sort_values("points", ascending=False).set_index("player_id")

  game=Game(game_config=GameConfig())
  game.play()
  return game#.dupe_winners

# NUM_GAMES = 30
# dupe_list = sc.parallelize(range(NUM_GAMES), numSlices=8).map(play_game).collect()
# dupes = sum(dupe_list)
# avgdupes = dupes/len(dupe_list)

# print(dupe_list)
# print(dupes)
# print(avgdupes)

# COMMAND ----------

print(dupe_list)

# COMMAND ----------

group = []
for i in range(50):
  game = play_game(i)
  group.append(game.leaderboard.drop(['skill'], axis = 1))

# display(game.leaderboard.head(10))
# game.leaderboard.points.head(5000).hist(bins=50)

# COMMAND ----------

import pandas as pd
grouped_frame = pd.concat(group)
top3 = grouped_frame.groupby(by=['player_id']).sum().sort_values("points", ascending=False)
top3.head(50).hist(bins=50)

# COMMAND ----------

top3 = grouped_frame.groupby(by=['player_id']).points.nlargest(3)


# COMMAND ----------

top_sum = top3.sum(level=0)
top_sum.head(100000).hist(bins=100)

# COMMAND ----------

grouped_frame

# COMMAND ----------

top3[117170].sum()
