{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fdefer-typed-holes #-}
module Compiler where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import qualified Scanner as S
import Control.Monad
import Data.List
import Data.Maybe

type Fields = [ByteString]
type Schema = [ByteString]
type Table = FilePath

data Record = Record { fields :: Fields, schema :: Schema } deriving Show


data Operator = Scan Table | Print Operator | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator

query = Project ["name"] ["name"] (Scan "data/test.csv")


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
evalPred  pred rec =
  case pred of
    Eq a b -> evalRef a rec == evalRef b rec
    Ne a b -> evalRef a rec /= evalRef b rec

evalRef :: Ref -> Record -> ByteString
evalRef (Value a) _ = a
evalRef (Field name) r = getField name r

getField :: ByteString -> Record -> ByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in fs !! i

restrict :: Record -> [ByteString] -> [ByteString] -> Record
restrict r newSchema parentSchema =
  Record (map (flip getField r) parentSchema) newSchema

execOp :: Operator -> (Record -> IO ()) -> IO ()
execOp op yld =
  case op of
    Scan filename -> processCSV filename yld
    Print p       -> execOp p (printFields . fields)
    Filter pred parent -> execOp parent (\rec -> when (evalPred pred rec) (yld rec))
    Project newSchema parentSchema parent -> execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in yld (Record (fields rec ++ fields rec')
                       (schema rec ++ schema rec'))))

main = do
  processCSV "data/test.csv" (print . getField "name")
  execOp (Print query) (\_ -> return ())



