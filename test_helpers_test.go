package main

import (
	"github.com/golang-jwt/jwt/v5"
)

// generateTestToken creates a JWT for testing.
func generateTestToken(accountID string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"account_id": accountID,
	})
	tokenString, _ := token.SignedString(jwtSecret)
	return tokenString
}
