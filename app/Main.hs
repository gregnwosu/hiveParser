{-# LANGUAGE DeriveFunctor #-}
module Main where

import Lib
import Text.Trifecta
import Control.Applicative
import Data.Maybe (fromMaybe)

path = "/Users/greg/dev/aimia/dataexploitation-messagestreambuilder/hdp-module/hive_tables/ddl_external_tables.sql"

data Line a = CKL a | OIL a | SQL a
          deriving (Show,Eq, Functor)
newtype  ExternalTableClause = ExternalTableClause [Line String]
    deriving (Show, Eq)
newtype OtherTypeClause = OtherTypeClause String
    deriving (Show, Eq)
data Clause = ETC ExternalTableClause | OTC OtherTypeClause
    deriving (Show, Eq)

skipEOL = skipMany (oneOf "\n \t")

sqlLine = some (noneOf ";\n")

specialLine s = try $ do
  prefix <- some (oneOf " \t,")
  cardkey <- string s
  suffix <- sqlLine
  return  (prefix ++ cardkey ++ suffix   )

clauseLines =  some $  (OIL <$> specialLine "offer_id" <|> CKL <$> specialLine "card_key" <|> SQL <$> sqlLine) <* skipEOL

otherTypeClauseParser = try $ do
    s <- sqlLine
    c <- optional $ string ";"
    skipEOL
    return (OtherTypeClause $  s++fromMaybe "" c)

externalTableClauseParser =  do
  preable <- (++) <$>  (many (oneOf " \t") *> string "CREATE EXTERNAL TABLE") <*> (some (notChar '\n') <* skipEOL)
  cl <- clauseLines
  let cinit = init cl
  let clast = last cl
  let newclast =  ( reverse . (';':) . reverse)  <$> clast
  char ';'
  skipEOL
  return $ ExternalTableClause $SQL  preable : reverse (  newclast: cinit)

sqlFileParser = some  $  (ETC <$>  externalTableClauseParser <|> OTC <$> otherTypeClauseParser) <* skipEOL

countTableClauses = foldr go 0
                    where go (ETC _) n = n + 1
                          go _ n = n
main = do
    sql <- readFile path
    putStr "number of table clauses is "
    let results = parseString sqlFileParser mempty sql
    print $ countTableClauses <$> results
