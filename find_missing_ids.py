import pandas as pd, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, '.')
from brain.retriever import _norm

members = pd.read_csv('data/real/members.csv', dtype=str)
members['_norm'] = members['Full_Name'].apply(_norm)

targets = [
    'Daniel Peppicelli', 'Jérémy Chauvy', 'Calixe Cathomen',
    'Nah Uel', 'Valentin Savioz', 'Thib Court', 'Matthieu Minguet',
    'Daniel Joseph', 'Jaumé Arranz', 'Othmar K', 'Charles Thoueille',
    'Daniel Grandjean', 'Joao Batista', 'Camille Couturier',
    'Greg Lcrx', 'Christian Petit', 'Baptiste Reichen',
    'Konstantin Dreyer', 'Jean-Luc Lebeau', 'Tom Norton'
]

found, missing = [], []
for name in targets:
    match = members[members['_norm'] == _norm(name)]
    if not match.empty:
        aid = str(match.iloc[0]['Athlete_ID'])
        if aid not in ('nan', '', 'None'):
            found.append((aid, name))
            continue
    missing.append(name)

print(f'Found IDs : {len(found)}')
print(f'Still missing : {len(missing)}')
print()
print('Run these:')
for aid, name in found:
    print(f'  python scrapers/scrape_followed_athletes.py --athlete {aid}  # {name}')
if missing:
    print()
    print('No ID found (not in club or left):')
    for name in missing:
        print(f'  {name}')
