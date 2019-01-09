#!/usr/bin/env cabal
{-# LANGUAGE OverloadedStrings #-}
{- cabal:
build-depends: base
-}
module Main where

import Control.Monad

header = "name,age"
file_lines = ["harry,25", "john,24", "tamara,32", "judith,43", "kirsty,20"]


main :: IO ()
main = forM_ [0..5] $ \i ->
          let n = 10 ^ i
              fname = "data-" ++ show n ++ ".csv"
              f = header : (take n (cycle file_lines))
          in writeFile fname (unlines f)

