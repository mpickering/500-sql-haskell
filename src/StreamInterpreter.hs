{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}
module StreamInterpreter where

import qualified Data.ByteString.Streaming as B
import qualified Data.ByteString.Streaming.Char8 as Q
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import Control.Monad
import Data.List
import Data.Maybe
import Streaming.Internal
import qualified Streaming.Prelude as S
import Streaming
import Control.Monad.Trans.Resource
import Debug.Trace
import qualified Data.ByteString.Short as B
         (ShortByteString, toShort, fromShort)

type Fields = [B.ShortByteString]
type Schema = [B.ShortByteString]
type Table = FilePath

data Record = Record { fields :: !Fields, schema :: !Schema }

data Operator = Scan Table | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator

query, query2 :: Operator
query = Project ["age"] ["age"] (Filter (Eq (Value "john") (Field "name")) (Scan "data/test.csv" ))

query2 = Project ["name"] ["name"] (Filter (Eq (Value "34") (Field "age")) (Scan "data/test.csv" ))

queryJoin :: Operator
queryJoin = Join (Scan "data/test.csv" ) (Scan "data/test1.csv" )

queryP :: Operator
queryP = Project ["name"] ["name"] (Scan "data/test.csv" )

data Predicate = Eq Ref Ref | Ne Ref Ref

data Ref = Field B.ShortByteString | Value B.ShortByteString

newtype Scanner = Scanner (BS ())

type BS a = Q.ByteString ResIO a

newScanner :: FilePath -> Scanner
newScanner fp = Scanner (Q.readFile fp)

nextLine :: Scanner -> Q.ByteString ResIO Scanner
nextLine (Scanner bs) =
  Scanner <$> Q.span (/= '\n') bs

hasNext :: Scanner -> ResIO (Of Bool Scanner)
hasNext (Scanner bs) = fmap Scanner <$> Q.testNull bs

data L m a = L [m a]

processCSV :: forall m . Monoid m => FilePath -> (Record -> ResIO m) -> ResIO m
processCSV fp yld = do
  scanner (newScanner fp)
  where
    scanner :: Scanner -> ResIO m
    scanner s = do
      fs :> rs <- Q.toStrict (nextLine s)
      let sch = map B.toShort (BC.split ',' fs)
      let f = rows sch rs
      destroy f (per_row sch) join (\_ -> pure mempty) -- (per_row sch yld) f

    per_row :: Schema -> Q.ByteString ResIO (ResIO m) -> ResIO m
    per_row fs s =
      let vs = S.toList $ mapped (fmap (first B.toShort) . Q.toStrict) $ Q.split ',' s
      in do
          a <- vs
          case a of
            ([""] :> m) -> m
            (bs :> m) -> yld (Record bs fs) <> m


    rows :: Schema -> Scanner -> Stream (Q.ByteString ResIO) ResIO ()
    rows fs (Scanner s) = Q.lines s

whenM :: Monoid m => Bool -> m -> m
whenM b act = if b then act else mempty


printFields :: Fields -> IO ()
printFields [] = putStr "\n"
printFields [!b] = BC.putStr (B.fromShort b) >> putStr "\n"
printFields (!b:bs) = BC.putStr (B.fromShort b) >> BC.putStr "," >> printFields bs

printFields2 [] = return ()
printFields2 (!b:bs) = printFields2 bs

evalPred :: Predicate -> Record -> Bool
evalPred  predicate rec =
  case predicate of
    Eq a b -> evalRef a rec == evalRef b rec
    Ne a b -> evalRef a rec /= evalRef b rec

evalRef :: Ref -> Record -> B.ShortByteString
evalRef (Value a) _ = a
evalRef (Field name) r = getField name r

getField :: B.ShortByteString -> Record -> B.ShortByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in fs !! i

getFields :: [B.ShortByteString] -> Record -> [B.ShortByteString]
getFields fs r = map (flip getField r) fs

restrict :: Record -> [B.ShortByteString] -> [B.ShortByteString] -> Record
restrict r newSchema parentSchema =
  Record (getFields parentSchema r) newSchema

execOp :: Monoid m => Operator -> (Record -> ResIO m) -> ResIO m
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
runQuery o = runResourceT $ execOp o (liftIO . printFields . fields)

runQueryL :: Operator -> IO [Record]
runQueryL o = runResourceT $ execOp o (return . return)

main :: IO ()
main = do
  runResourceT $ processCSV "data/test.csv" (liftIO . print . getField "name")
  runQuery query


instance Semigroup m => Semigroup (ResourceT IO m) where
  (<>) a1 a2 = do
    (<>) <$> a1 <*> a2

instance Monoid m => Monoid (ResourceT IO m) where
  mempty = pure mempty


