# AP & Timestep
MAX_AP = 15
STEPS = 20

# Time Window Training Dataset
YEARS_TRAIN = 1
MONTHS_TRAIN = 0

# Airlines
AIRLINES = {
	'CL': ['Sky', 'JetSmart'],
	'AR': ['Aerolineas Argentinas', 'Andes Lineas Aereas', 'Flybondi', 'Norwegian'],
	'CO': ['Avianca', 'VivaColombia', 'Wingo', 'Copa Air', 'Viva Air'],
	'EC': ['Tame Linea Aerea', 'Avianca'],
	'PE': ['Peruvian Airlines', 'Sky', 'LC Peru', 'Peruvian Airlines', 'Avianca', 'Star Peru', 'Viva Air'],
	'BR': ['Gol', 'Azul']
}

# XGBoost Hyperparameters
XGB_PARAMS = {
	'CL': {
		'max_depth': 3,
		'colsample_bytree': 0.8,
		'subsample': 0.5,
		'learning_rate': 0.05,
		'num_boost_round': 1500
	},
	'EC': {
		'max_depth': 3,
		'colsample_bytree': 0.6,
		'subsample': 0.6,
		'learning_rate': 0.05,
		'num_boost_round': 1050
	},
	'PE': {
		'max_depth': 3,
		'colsample_bytree': 0.5,
		'subsample': 0.5,
		'learning_rate': 0.01,
		'num_boost_round': 1100
	},
	'AR': {
		'max_depth': 3,
		'colsample_bytree': 0.6,
		'subsample': 0.6,
		'learning_rate': 0.01,
		'num_boost_round': 1300
	},
	'CO': {
		'max_depth': 3,
		'colsample_bytree': 0.8,
		'subsample': 0.5,
		'learning_rate': 0.05,
		'num_boost_round': 1400
	},
	'BR': {
		'max_depth': 3,
		'colsample_bytree': 0.5,
		'subsample': 0.4,
		'learning_rate': 0.2,
		'num_boost_round': 200
	}
}