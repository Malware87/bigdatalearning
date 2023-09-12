import pandas as pd

# Đọc dữ liệu từ tệp CSV
df_qg = pd.read_csv('vdv_olympics.csv')
df_qg_noc = pd.read_csv('qg_noc.csv')

# Câu 6: Số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21.
male_athletes_21st_century = df_qg_noc[(df_qg_noc['Sex'] == 'M') & (
    df_qg_noc['Year'] >= 2000) & (df_qg_noc['Year'] <= 2021)]
male_athletes_count_21st_century = male_athletes_21st_century.groupby('Year')[
    'ID'].count().reset_index()
print("Câu 6: Số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21:")
print(male_athletes_count_21st_century)
print()

# Câu 7: Tổng số lượng vận động viên tại mỗi kỳ thế vận hội trong thập kỷ 1990 của Nga.
russian_athletes_1990s = df_qg_noc[(df_qg_noc['Team'] == 'Russia') & (
    df_qg_noc['Year'] >= 1990) & (df_qg_noc['Year'] <= 1999)]
russian_athletes_count_1990s = russian_athletes_1990s.groupby('Year')[
    'ID'].count().reset_index()
print("Câu 7: Tổng số lượng vận động viên tại mỗi kỳ thế vận hội trong thập kỷ 1990 của Nga:")
print(russian_athletes_count_1990s)
print()

# Câu 8: Chiều cao tối thiểu, trung bình và tối đa của mỗi quốc gia tham gia thế vận hội mùa đông và sắp xếp theo thứ tự giảm dần.
winter_athletes_height = df_qg_noc[df_qg_noc['Season'] == 'Winter']
winter_athletes_height_stats = winter_athletes_height.groupby(
    'Team')['Height'].agg(['min', 'mean', 'max']).reset_index()
winter_athletes_height_stats = winter_athletes_height_stats.sort_values(
    by='mean', ascending=False)
print("Câu 8: Chiều cao tối thiểu, trung bình và tối đa của mỗi quốc gia tham gia thế vận hội mùa đông và sắp xếp theo thứ tự giảm dần:")
print(winter_athletes_height_stats)
print()

# Câu 9: Đưa ra tên các quốc gia không có thông tin về quốc gia trong qg_noc.
countries_missing_noc = df_qg_noc[df_qg_noc['NOC'].isnull()]['Team'].unique()
print("Câu 9: Tên các quốc gia không có thông tin về quốc gia trong qg_noc:")
print(countries_missing_noc)
print()

# Câu 10: Liệt kê 10 vận động viên có giành nhiều huy chương vàng nhất thế kỷ 20.
athletes_20th_century = df_qg_noc[(
    df_qg_noc['Year'] >= 1900) & (df_qg_noc['Year'] <= 1999)]
gold_medalists_20th_century = athletes_20th_century[athletes_20th_century['Medal'] == 'Gold']
top_10_gold_medalists_20th_century = gold_medalists_20th_century.groupby('Name')[
    'Medal'].count().reset_index()
top_10_gold_medalists_20th_century = top_10_gold_medalists_20th_century.sort_values(
    by='Medal', ascending=False).head(10)
print("Câu 10: Danh sách 10 vận động viên có giành nhiều huy chương vàng nhất thế kỷ 20:")
print(top_10_gold_medalists_20th_century)
