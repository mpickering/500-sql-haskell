#!/usr/bin/env cabal
{- cabal:
build-depends: base, random-strings, streaming-bytestring, bytestring, streaming, resourcet
-}
module Main where

import Control.Monad
import Data.List
import Test.RandomStrings
import qualified Data.ByteString.Streaming.Char8 as Q
import qualified Streaming.Prelude as S
import Streaming
import qualified Streaming.Internal as I
import Control.Monad.Trans.Resource

c_limit = 2 ^ c_limit_k

c_limit_k = 4

file_line :: Int -> Q.ByteString ResIO ()
file_line l =
  Q.mwrap (liftIO $ Q.string . intercalate "," <$> randomStringsLen (randomString (onlyAlpha randomASCII)) (1, 5) l)

{-
main :: IO ()
main =

  forM_ [0..c_limit_k] $ \c ->
    let cols = 10 ^ c
        headers = ["f" ++ show n | n <- [0..cols] ]
        header = intercalate "," headers
    in forM_ [0..5] $ \i -> do
          let n = 10 ^ i
              fname = "data-" ++ show n ++ "-" ++ show cols ++ ".csv"
          file_lines <- replicateM n (file_line cols)
          let f = header : file_lines
          writeFile fname (unlines f)
          -}

main :: IO ()
main = runResourceT $

  forM_ [0..c_limit_k] $ \c ->
    let cols = 2 ^ c
        header :: Q.ByteString ResIO ()
        header = Q.pack $
                  intercalates (S.yield ',') $
                    S.subst (\n -> S.yield 'f'  >> S.each (show n)) (S.take cols $ S.enumFrom 0)
    in forM_ [0..5] $ \i -> do
          let n = 10 ^ i
              fname = "data-" ++ show n ++ "-" ++ show cols ++ ".csv"
          let file_lines = I.replicates n (file_line cols)
          let f = do I.yields header >> file_lines
          liftIO $ print fname
          Q.writeFile fname (Q.unlines f)

