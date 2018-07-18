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



data Operator = Scan Table | Print Operator | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator

query :: Operator
query = Project ["name"] ["name"] (Scan "data/test.csv")

queryJoin :: Operator
queryJoin = Join (Scan "data/test.csv") (Scan "data/test1.csv")


data Predicate = Eq Ref Ref | Ne Ref Ref

data Ref = Field ByteString | Value ByteString

processCSV :: FilePath -> (Record -> IO ()) -> IO ()
processCSV fp yld = do
  s <- B.readFile fp
  scanner s

  where
    scanner s = do
      let (fs, rs) = BC.span (/= '\n') s
      rows (BC.split ',' fs) (BC.tail rs)

    rows fs rs =
      case rs of
        "" -> return ()
        _ -> do
          let (r, rest) = BC.span (/= '\n') rs
          yld (Record (BC.split ',' r) fs)
          rows fs (BC.tail rest)


printFields :: Fields -> IO ()
printFields = print

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

execOp :: Operator -> (Record -> IO ()) -> IO ()
execOp op yld =
  case op of
    Scan filename -> processCSV filename yld
    Print p       -> execOp p (printFields . fields)
    Filter predicate parent -> execOp parent (\rec -> when (evalPred predicate rec) (yld rec))
    Project newSchema parentSchema parent -> execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in when (getFields keys rec == getFields keys rec')
            (yld (Record (fields rec ++ fields rec')
                       (schema rec ++ schema rec')))))


runQuery :: Operator -> IO ()
runQuery o = execOp (Print o) (\_ -> return ())

main :: IO ()
main = do
  processCSV "data/test.csv" (print . getField "name")
  execOp (Print query) (\_ -> return ())



