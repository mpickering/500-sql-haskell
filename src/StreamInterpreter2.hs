{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
module StreamInterpreter2 where

-- Like stream interpreter but with a different yield function

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

data OfBSG m a b = OfBS !a (Q.ByteString m b) deriving (Functor)

type OfBS a b = OfBSG ResIO a b

getBS :: OfBS a b -> Q.ByteString ResIO b
getBS (OfBS a b) = b

-- How to parse one field, and the result of parsing the rest of the file
type Fields a = Stream (OfBSG ResIO B.ShortByteString) ResIO a
type Schema = [B.ShortByteString]
type Table = FilePath

data Record a = Record { fields :: !(Fields a), schema :: !Schema }

data Operator = Scan Table | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator

query, query2 :: Operator
query = Project ["age"] ["age"] (Filter (Eq (Value "john") (Field "name")) (Scan "data/test.csv" ))

query2 = Project ["name"] ["name"] (Filter (Eq (Value "34") (Field "age")) (Scan "data/test.csv" ))

queryJoin :: Operator
queryJoin = Join (Scan "data/test.csv" ) (Scan "data/test1.csv" )

queryP :: Operator
queryP = Project ["name"] ["name"] (Scan "data/test.csv" )

queryF = (Filter (Eq (Value "john") (Field "name")) (Scan "data/test.csv" ))

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

processCSV :: forall m . Monoid m => FilePath -> (Record (ResIO m) -> ResIO m) -> ResIO m
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
      let vs = Q.split ',' s
      in yld $ Record (zipWithStream (OfBS) fs vs) fs


    rows :: Schema -> Scanner -> Stream (Q.ByteString ResIO) ResIO ()
    rows fs (Scanner s) = Q.lines s

zipWithStream
  :: (Monad m, Functor n)
  =>  (forall x . a -> Q.ByteString m x -> n x)
  -> [a]
  -> Stream (Q.ByteString m) m r
  -> Stream n m r
zipWithStream op zs = loop zs
  where
    loop [] !ls      = loop zs ls
    loop a@(x:xs)  ls = case ls of
      Return r -> Return r
      Step fls -> Step $ fmap (loop xs) (op x fls)
      Effect mls -> Effect $ liftM (loop a) mls

{-#INLINABLE zipWithStream #-}

whenM :: (Monad n, Monoid m) => n Bool -> n m -> n m
whenM ba act = ba >>= \b -> if b then act else return mempty


printFields :: Fields (ResIO r) -> ResIO r
printFields s = join (Q.putStrLn (Q.intercalate "," (maps getBS s)))
{-
printFields [] = putStr "\n"
printFields [!b] = BC.putStr (B.fromShort b) >> putStr "\n"
printFields (!b:bs) = BC.putStr (B.fromShort b) >> BC.putStr "," >> printFields bs
-}

printFields2 [] = return ()
printFields2 (!b:bs) = printFields2 bs

evalPred :: Predicate -> Record (ResIO a)
         -> (Record (ResIO m) -> ResIO m)
         -> ResIO m
evalPred  predicate (Record d_fs sch) k = do
  let r = toListBS $ d_fs

  return _
--  k (Record s sch)

  where
    go (fs :> s) =  _
    pur_go fs =
      case predicate of
        Eq a b -> evalRef a fs sch == evalRef b fs sch
        Ne a b -> evalRef a fs sch /= evalRef b fs sch

evalRef :: Ref -> [B.ShortByteString] -> [B.ShortByteString] -> B.ShortByteString
evalRef (Value a) _ _ = a
evalRef (Field name) fs sch = getField name fs sch

-- Copy and read all the fields in a stream
toListBS' :: forall a b . (Stream (OfBSG ResIO a) ResIO b)
         -> ResIO (Of [B.ShortByteString] (Stream (OfBSG ResIO a) ResIO b))
toListBS' s = destroy s _ _ _
  where
    go :: OfBSG
               ResIO
               a
               (ResIO (Of [B.ShortByteString] (Stream (OfBSG ResIO a) ResIO b)))
             -> ResIO (Of [B.ShortByteString] (Stream (OfBSG ResIO a) ResIO b))
    go (OfBS a b) = do
      let v = Q.toStrict (Q.copy b)
--      bs :> k2 <- k
      return _ --((B.toShort b : bs) :> k2)


-- Copy and read all the fields in a stream
toListBS :: forall a b . (Stream (OfBSG ResIO a) ResIO b)
         -> Stream (OfBSG ResIO a) ResIO (Of [B.ShortByteString] b)
toListBS s = destroy s go effect (\b -> return ([] :> b))
  where
    go :: OfBSG
               ResIO a (Stream (OfBSG ResIO a) ResIO (Of [B.ShortByteString] b))
             -> Stream (OfBSG ResIO a) ResIO (Of [B.ShortByteString] b)

    go (OfBS a b) = do
      let v = Q.toStrict (Q.copy b)
          g (bs :> s) = fmap (\(bss :> b) -> (B.toShort bs: bss) :> b) s
      wrap (OfBS a (fmap g v))
--      ls :> b' <- fs
--      return ((B.toShort l : ls) :> b')

getField :: B.ShortByteString -> [B.ShortByteString]
         -> [B.ShortByteString] -> B.ShortByteString
getField field fs sch =
  let i = fromJust (elemIndex field sch)
  in fs !! i

getFields :: [B.ShortByteString] -> Record a -> ResIO [B.ShortByteString]
getFields fs (Record d_fs sch) = do
  fs <- toListBS d_fs
  return $ map (\f -> getField f fs sch) fs

restrict :: Record a -> [B.ShortByteString] -> [B.ShortByteString] -> Record a
restrict r newSchema parentSchema =
  Record (filterQ  (`elem` parentSchema) (fields r)) newSchema

-- | Skip elements of a stream that fail a predicate
filterQ  :: forall a r m . Monad m => (a -> Bool) -> Stream (OfBSG m a) m r -> Stream (OfBSG m a) m r
filterQ thePred = loop where
  loop :: Monad m => Stream (OfBSG m a) m r -> Stream (OfBSG m a) m r
  loop str = case str of
    Return r       -> Return r
    Effect m       -> Effect (fmap loop m)
    Step (a `OfBS` as) -> if thePred a
      then Step (a `OfBS` (fmap loop as))
      else Step (a `OfBS` lift (Q.effects (fmap loop as)))


execOp :: Monoid m => Operator -> (Record (ResIO m) -> ResIO m) -> ResIO m
execOp op yld =
  case op of
    Scan filename -> processCSV filename yld
    Filter predicate parent -> execOp parent (\rec -> (evalPred predicate rec) yld)
    Project newSchema parentSchema parent -> execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in whenM ((==) <$> (getFields keys rec) <*> (getFields keys rec'))
            (yld (Record (fields rec <> fields rec')
                       (schema rec ++ schema rec')))))


runQuery :: Operator -> IO ()
runQuery o = runResourceT $ execOp o (printFields . fields)

{-
runQueryL :: Operator -> IO [Record ()]
runQueryL o = runResourceT $ execOp o _
-}

main :: IO ()
main = do
--  runResourceT $ processCSV "data/test.csv" (liftIO . print . getField "name")
  runQuery query


instance Semigroup m => Semigroup (ResourceT IO m) where
  (<>) a1 a2 = do
    (<>) <$> a1 <*> a2

instance Monoid m => Monoid (ResourceT IO m) where
  mempty = pure mempty


