CREATE (УстиновскийФАП:ФАП{name: "Устиновский ФАП", YOB: 1985, POB: "пгт.Устиново"})
CREATE (Ind:Country {name: "India"})
CREATE (Dhawan)-[r:BATSMAN_OF]->(Ind)