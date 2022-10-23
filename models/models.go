package models

type T1 struct {
	ID string `json:"id" db:"id" gorm:"primaryKey"`
}

type T2 struct {
	ID string `json:"id" db:"id" gorm:"primaryKey"`
}

type T3 struct {
	ID string `json:"id" db:"id" gorm:"primaryKey"`
}
