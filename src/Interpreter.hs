{-# LANGUAGE OverloadedStrings #-}
module Interpreter where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import Control.Monad
import Data.List
import Data.Maybe

type Fields = [ByteString]
type Schema = [ByteString]
type Table = FilePath

data Record = Record { fields :: Fields, schema :: Schema } deriving Show

data Operator = Scan Table | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator

query :: Operator
query = Project ["name"] ["name"] (Scan "data/test.csv")

queryJoin :: Operator
queryJoin = Join (Scan "data/test.csv") (Scan "data/test1.csv")


data Predicate = Eq Ref Ref | Ne Ref Ref

data Ref = Field ByteString | Value ByteString

newtype Scanner = Scanner ByteString

newScanner :: FilePath -> IO Scanner
newScanner fp = Scanner <$>  B.readFile fp

nextLine :: Scanner -> (ByteString, Scanner)
nextLine (Scanner bs) =
  let (fs, rs) = BC.span (/= '\n') bs
  in (fs, Scanner (BC.tail rs))

hasNext :: Scanner -> Bool
hasNext (Scanner bs) = bs /= ""

processCSV :: Monoid m => FilePath -> (Record -> IO m) -> IO m
processCSV fp yld = do
  s <- newScanner fp
  scanner s

  where
    scanner s = do
      let (fs, rs) = nextLine s
      rows (BC.split ',' fs) rs

    rows fs s =
      whenM (hasNext s)
        (let (r, rest) = nextLine s
          in yld (Record (BC.split ',' r) fs)
             <> rows fs rest)

whenM :: Monoid m => Bool -> m -> m
whenM b act = if b then act else mempty


printFields :: Fields -> IO ()
printFields [] = putStr "\n"
printFields [b] = BC.putStr b >> putStr "\n"
printFields (b:bs) = BC.putStr b >> BC.putStr "," >> printFields bs

evalPred :: Predicate -> Record -> Bool
evalPred  predicate rec =
  case predicate of
    Eq a b -> evalRef a rec == evalRef b rec
    Ne a b -> evalRef a rec /= evalRef b rec

evalRef :: Ref -> Record -> ByteString
evalRef (Value a) _ = a
evalRef (Field name) r = getField name r

getField :: ByteString -> Record -> ByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in fs !! i

getFields :: [ByteString] -> Record -> [ByteString]
getFields fs r = map (flip getField r) fs

restrict :: Record -> [ByteString] -> [ByteString] -> Record
restrict r newSchema parentSchema =
  Record (getFields parentSchema r) newSchema

execOp :: Monoid m => Operator -> (Record -> IO m) -> IO m
execOp op yld =
  case op of
    Scan filename -> processCSV filename yld
    Filter predicate parent -> execOp parent (\rec -> whenM (evalPred predicate rec) (yld rec))
    Project newSchema parentSchema parent -> execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in whenM (getFields keys rec == getFields keys rec')
            (yld (Record (fields rec ++ fields rec')
                       (schema rec ++ schema rec')))))


runQuery :: Operator -> IO ()
runQuery o = execOp o (printFields . fields)

runQueryL :: Operator -> IO [Record]
runQueryL o = execOp o (return . return)

main :: IO ()
main = do
  processCSV "data/test.csv" (print . getField "name")
  runQuery query



